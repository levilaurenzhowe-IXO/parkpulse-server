const express = require('express');
const cors = require('cors');
const cron = require('node-cron');
const fetch = require('node-fetch');
const { createClient } = require('@libsql/client');

const app = express();
const PORT = process.env.PORT || 3000;

app.use(cors());
app.use(express.json());

// ---------- TURSO DATENBANK VERBINDUNG ----------
const db = createClient({
  url: process.env.TURSO_DATABASE_URL,
  authToken: process.env.TURSO_AUTH_TOKEN,
});

const PARKS = [
  { id: '60', name: 'Kings Island' },
  { id: '64', name: 'Cedar Point' },
  { id: '56', name: 'Phantasialand' }
];

let lastFetchTimestamp = 0;

const PARK_LATITUDE = 50.801472;
const PARK_LONGITUDE = 6.876355;

let currentWeatherCache = null;
let currentSchoolHolidayCache = null;

async function initDatabase() {
  await db.execute(`
    CREATE TABLE IF NOT EXISTS wait_times (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      park_id TEXT NOT NULL,
      ride_id TEXT,
      ride_name TEXT NOT NULL,
      is_open INTEGER NOT NULL,
      wait_time INTEGER NOT NULL,
      recorded_at INTEGER NOT NULL,
      recorded_date TEXT NOT NULL,
      recorded_time TEXT NOT NULL,
      weekday INTEGER NOT NULL
    )
  `);

  const newColumns = [
    { name: 'temperature', type: 'REAL' },
    { name: 'precipitation', type: 'REAL' },
    { name: 'weather_code', type: 'INTEGER' },
    { name: 'is_school_holiday', type: 'INTEGER' },
    { name: 'holiday_countries', type: 'TEXT' },
    { name: 'is_public_holiday', type: 'INTEGER' }
  ];
  for (const col of newColumns) {
    try {
      await db.execute(`ALTER TABLE wait_times ADD COLUMN ${col.name} ${col.type}`);
    } catch (err) {}
  }

  await db.execute(`CREATE INDEX IF NOT EXISTS idx_park_date ON wait_times (park_id, recorded_date)`);
  await db.execute(`CREATE INDEX IF NOT EXISTS idx_ride_name ON wait_times (park_id, ride_name)`);

  await db.execute(`
    CREATE TABLE IF NOT EXISTS hidden_rides (
      park_id TEXT NOT NULL,
      ride_name TEXT NOT NULL,
      PRIMARY KEY (park_id, ride_name)
    )
  `);

  console.log('✅ Datenbank-Schema bereit.');
}

async function fetchCurrentWeather() {
  try {
    const url = `https://api.open-meteo.com/v1/forecast?latitude=${PARK_LATITUDE}&longitude=${PARK_LONGITUDE}&current=temperature_2m,precipitation,weather_code&timezone=Europe%2FBerlin`;
    const res = await fetch(url);
    if (!res.ok) return null;

    const data = await res.json();
    if (!data.current) return null;

    return {
      temperature: data.current.temperature_2m,
      precipitation: data.current.precipitation,
      weatherCode: data.current.weather_code
    };
  } catch (err) {
    console.error('Fehler beim Wetterabruf:', err.message);
    return null;
  }
}

const HOLIDAY_REGIONS = [
  { country: 'DE', subdivision: 'DE-NW', label: 'Deutschland (NRW)' },
  { country: 'NL', subdivision: null, label: 'Niederlande' },
  { country: 'BE', subdivision: null, label: 'Belgien' },
  { country: 'FR', subdivision: null, label: 'Frankreich' },
  { country: 'LU', subdivision: null, label: 'Luxemburg' }
];

let holidayCache = { schoolHolidays: [], publicHolidays: [], lastFetched: 0 };

async function fetchHolidaysForRegion(endpoint, region, validFrom, validTo) {
  try {
    let url = `https://openholidaysapi.org/${endpoint}?countryIsoCode=${region.country}&validFrom=${validFrom}&validTo=${validTo}&languageIsoCode=DE`;
    if (region.subdivision) url += `&subdivisionCode=${region.subdivision}`;

    const res = await fetch(url);
    if (!res.ok) return [];

    const data = await res.json();
    return (Array.isArray(data) ? data : []).map(h => ({
      startDate: h.startDate,
      endDate: h.endDate,
      name: h.name && h.name[0] ? h.name[0].text : (h.name || 'Unbekannt'),
      country: region.label
    }));
  } catch (err) {
    console.error(`Fehler beim Abruf ${endpoint} für ${region.label}:`, err.message);
    return [];
  }
}

async function refreshHolidayCache() {
  console.log('Aktualisiere Ferien-/Feiertagsdaten für alle Regionen...');

  const now = new Date();
  const validFrom = now.toLocaleDateString('sv-SE', { timeZone: 'Europe/Berlin' });
  const future = new Date(now);
  future.setFullYear(future.getFullYear() + 1);
  const validTo = future.toLocaleDateString('sv-SE', { timeZone: 'Europe/Berlin' });

  const allSchoolHolidays = [];
  const allPublicHolidays = [];

  for (const region of HOLIDAY_REGIONS) {
    const school = await fetchHolidaysForRegion('SchoolHolidays', region, validFrom, validTo);
    const pub = await fetchHolidaysForRegion('PublicHolidays', region, validFrom, validTo);
    allSchoolHolidays.push(...school);
    allPublicHolidays.push(...pub);
  }

  holidayCache = {
    schoolHolidays: allSchoolHolidays,
    publicHolidays: allPublicHolidays,
    lastFetched: Date.now()
  };

  console.log(`-> ${allSchoolHolidays.length} Schulferien-Zeiträume, ${allPublicHolidays.length} Feiertage geladen.`);
}

function getSchoolHolidayInfo(dateStr) {
  const matches = holidayCache.schoolHolidays.filter(h => dateStr >= h.startDate && dateStr <= h.endDate);
  return {
    isHoliday: matches.length > 0,
    countries: [...new Set(matches.map(m => m.country))]
  };
}

function getPublicHolidayInfo(dateStr) {
  const matches = holidayCache.publicHolidays.filter(h => h.startDate === dateStr);
  return {
    isHoliday: matches.length > 0,
    countries: [...new Set(matches.map(m => m.country))],
    names: matches.map(m => m.name)
  };
}

cron.schedule('0 3 * * *', () => {
  refreshHolidayCache();
});

async function fetchAndSaveData() {
  console.log(`[${new Date().toISOString()}] Starte Datenabruf für alle Parks...`);

  currentWeatherCache = await fetchCurrentWeather();

  const now = new Date();
  const recordedDate = now.toLocaleDateString('sv-SE', { timeZone: 'Europe/Berlin' });
  const recordedTime = now.toLocaleTimeString('de-DE', { timeZone: 'Europe/Berlin', hour: '2-digit', minute: '2-digit' });
  const weekday = new Date(now.toLocaleString('en-US', { timeZone: 'Europe/Berlin' })).getDay();

  if (holidayCache.lastFetched === 0) {
    await refreshHolidayCache();
  }
  const schoolHolidayInfo = getSchoolHolidayInfo(recordedDate);
  const publicHolidayInfo = getPublicHolidayInfo(recordedDate);

  for (const park of PARKS) {
    try {
      const res = await fetch(`https://queue-times.com/parks/${park.id}/queue_times.json`);

      if (!res.ok) continue;

      const data = await res.json();
      let rides = [];
      if (data.rides) rides.push(...data.rides);
      if (data.lands) {
        data.lands.forEach(land => {
          if (land.rides) rides.push(...land.rides);
        });
      }

      const statements = rides.map(r => ({
        sql: `INSERT INTO wait_times
              (park_id, ride_id, ride_name, is_open, wait_time, recorded_at, recorded_date, recorded_time, weekday,
               temperature, precipitation, weather_code, is_school_holiday, holiday_countries, is_public_holiday)
              VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        args: [
          park.id,
          String(r.id || ''),
          r.name,
          r.is_open ? 1 : 0,
          r.wait_time || 0,
          Date.now(),
          recordedDate,
          recordedTime,
          weekday,
          currentWeatherCache ? currentWeatherCache.temperature : null,
          currentWeatherCache ? currentWeatherCache.precipitation : null,
          currentWeatherCache ? currentWeatherCache.weatherCode : null,
          schoolHolidayInfo.isHoliday ? 1 : 0,
          schoolHolidayInfo.countries.join(','),
          publicHolidayInfo.isHoliday ? 1 : 0
        ]
      }));

      if (statements.length > 0) {
        await db.batch(statements, 'write');
      }

      console.log(`-> ${park.name} (${park.id}): ${rides.length} Attraktionen gespeichert.`);

    } catch (err) {
      console.error(`Fehler beim Abruf für ${park.name}:`, err.message);
    }
  }

  lastFetchTimestamp = Date.now();
}

cron.schedule('*/15 * * * *', () => {
  fetchAndSaveData();
});

app.get('/health', (req, res) => {
  res.status(200).json({
    status: 'ok',
    timestamp: new Date().toISOString(),
    lastDataFetch: lastFetchTimestamp ? new Date(lastFetchTimestamp).toISOString() : null
  });
});

app.get('/api/context', async (req, res) => {
  try {
    if (holidayCache.lastFetched === 0) {
      await refreshHolidayCache();
    }

    const now = new Date();
    const today = now.toLocaleDateString('sv-SE', { timeZone: 'Europe/Berlin' });

    const schoolHolidayInfo = getSchoolHolidayInfo(today);
    const publicHolidayInfo = getPublicHolidayInfo(today);

    let weather = currentWeatherCache;
    if (!weather) {
      weather = await fetchCurrentWeather();
    }

    res.json({
      weather,
      today: {
        date: today,
        isSchoolHoliday: schoolHolidayInfo.isHoliday,
        schoolHolidayCountries: schoolHolidayInfo.countries,
        isPublicHoliday: publicHolidayInfo.isHoliday,
        publicHolidayNames: publicHolidayInfo.names,
        publicHolidayCountries: publicHolidayInfo.countries
      },
      schoolHolidays: holidayCache.schoolHolidays,
      publicHolidays: holidayCache.publicHolidays,
      lastRefreshed: holidayCache.lastFetched ? new Date(holidayCache.lastFetched).toISOString() : null
    });

  } catch (err) {
    console.error('Fehler in /api/context:', err.message);
    res.status(500).json({ error: 'Serverfehler.' });
  }
});

app.get('/api/park', async (req, res) => {
  const parkId = req.query.park || '56';

  try {
    if (Date.now() - lastFetchTimestamp > 10 * 60 * 1000) {
      console.log('Server war inaktiv, hole frische Daten...');
      await fetchAndSaveData();
    }

    const today = new Date().toLocaleDateString('sv-SE', { timeZone: 'Europe/Berlin' });

    const result = await db.execute({
      sql: `SELECT * FROM wait_times WHERE park_id = ? AND recorded_date = ? ORDER BY recorded_at ASC`,
      args: [parkId, today]
    });

    const hiddenResult = await db.execute({
      sql: `SELECT ride_name FROM hidden_rides WHERE park_id = ?`,
      args: [parkId]
    });
    const hiddenNames = new Set(hiddenResult.rows.map(r => r.ride_name));

    const grouped = {};
    for (const row of result.rows) {
      if (hiddenNames.has(row.ride_name)) continue;

      if (!grouped[row.recorded_at]) {
        grouped[row.recorded_at] = {
          time: row.recorded_time,
          timestamp: row.recorded_at,
          rides: []
        };
      }
      grouped[row.recorded_at].rides.push({
        name: row.ride_name,
        isOpen: !!row.is_open,
        waitTime: row.wait_time
      });
    }

    const history = Object.values(grouped).sort((a, b) => a.timestamp - b.timestamp);

    res.json({ history, hiddenRides: Array.from(hiddenNames) });

  } catch (err) {
    console.error('Fehler in /api/park:', err.message);
    res.status(500).json({ error: 'Serverfehler beim Abrufen der Daten.' });
  }
});

app.post('/api/hidden-rides', async (req, res) => {
  const { parkId, rideName, hidden } = req.body;

  if (!parkId || !rideName) {
    return res.status(400).json({ error: 'parkId und rideName erforderlich.' });
  }

  try {
    if (hidden) {
      await db.execute({
        sql: `INSERT OR IGNORE INTO hidden_rides (park_id, ride_name) VALUES (?, ?)`,
        args: [parkId, rideName]
      });
    } else {
      await db.execute({
        sql: `DELETE FROM hidden_rides WHERE park_id = ? AND ride_name = ?`,
        args: [parkId, rideName]
      });
    }
    res.json({ success: true });
  } catch (err) {
    console.error('Fehler in /api/hidden-rides:', err.message);
    res.status(500).json({ error: 'Serverfehler.' });
  }
});

app.get('/api/stats', async (req, res) => {
  const parkId = req.query.park || '56';
  const rideName = req.query.ride;

  try {
    let sql = `
      SELECT
        ride_name,
        weekday,
        CAST(substr(recorded_time, 1, 2) AS INTEGER) as hour,
        AVG(wait_time) as avg_wait,
        COUNT(*) as sample_count
      FROM wait_times
      WHERE park_id = ? AND is_open = 1
    `;
    const args = [parkId];

    if (rideName) {
      sql += ` AND ride_name = ?`;
      args.push(rideName);
    }

    sql += ` GROUP BY ride_name, weekday, hour ORDER BY ride_name, weekday, hour`;

    const result = await db.execute({ sql, args });

    res.json({ stats: result.rows });

  } catch (err) {
    console.error('Fehler in /api/stats:', err.message);
    res.status(500).json({ error: 'Serverfehler bei Statistik-Abfrage.' });
  }
});

app.get('/api/rides-list', async (req, res) => {
  const parkId = req.query.park || '56';

  try {
    const result = await db.execute({
      sql: `SELECT DISTINCT ride_name FROM wait_times WHERE park_id = ? ORDER BY ride_name ASC`,
      args: [parkId]
    });
    res.json({ rides: result.rows.map(r => r.ride_name) });
  } catch (err) {
    console.error('Fehler in /api/rides-list:', err.message);
    res.status(500).json({ error: 'Serverfehler.' });
  }
});

app.get('/api/daily-stats', async (req, res) => {
  const parkId = req.query.park || '56';
  const days = Math.min(parseInt(req.query.days, 10) || 30, 90);

  try {
    const cutoff = new Date();
    cutoff.setDate(cutoff.getDate() - days);
    const cutoffDate = cutoff.toLocaleDateString('sv-SE', { timeZone: 'Europe/Berlin' });

    const dailySummaryResult = await db.execute({
      sql: `
        SELECT
          recorded_date,
          AVG(CASE WHEN is_open = 1 THEN wait_time ELSE NULL END) as avg_wait,
          COUNT(DISTINCT recorded_at) as sample_points,
          MAX(CASE WHEN is_open = 1 THEN wait_time ELSE 0 END) as peak_wait
        FROM wait_times
        WHERE park_id = ? AND recorded_date >= ?
        GROUP BY recorded_date
        ORDER BY recorded_date ASC
      `,
      args: [parkId, cutoffDate]
    });

    const perRideDailyResult = await db.execute({
      sql: `
        SELECT
          recorded_date,
          ride_name,
          AVG(CASE WHEN is_open = 1 THEN wait_time ELSE NULL END) as avg_wait,
          MAX(CASE WHEN is_open = 1 THEN wait_time ELSE 0 END) as peak_wait
        FROM wait_times
        WHERE park_id = ? AND recorded_date >= ?
        GROUP BY recorded_date, ride_name
        ORDER BY recorded_date ASC, ride_name ASC
      `,
      args: [parkId, cutoffDate]
    });

    const averageDayResult = await db.execute({
      sql: `
        SELECT
          recorded_time,
          AVG(CASE WHEN is_open = 1 THEN wait_time ELSE NULL END) as avg_wait,
          COUNT(DISTINCT recorded_date) as days_counted
        FROM wait_times
        WHERE park_id = ? AND recorded_date >= ?
        GROUP BY recorded_time
        ORDER BY recorded_time ASC
      `,
      args: [parkId, cutoffDate]
    });

    const averageDayPerRideResult = await db.execute({
      sql: `
        SELECT
          recorded_time,
          ride_name,
          AVG(CASE WHEN is_open = 1 THEN wait_time ELSE NULL END) as avg_wait
        FROM wait_times
        WHERE park_id = ? AND recorded_date >= ?
        GROUP BY recorded_time, ride_name
        ORDER BY recorded_time ASC, ride_name ASC
      `,
      args: [parkId, cutoffDate]
    });

    res.json({
      dailySummary: dailySummaryResult.rows,
      perRideDaily: perRideDailyResult.rows,
      averageDay: averageDayResult.rows,
      averageDayPerRide: averageDayPerRideResult.rows
    });

  } catch (err) {
    console.error('Fehler in /api/daily-stats:', err.message);
    res.status(500).json({ error: 'Serverfehler bei Tages-Statistik-Abfrage.' });
  }
});

app.get('/api/ride-days', async (req, res) => {
  const parkId = req.query.park || '56';
  const rideName = req.query.ride;

  if (!rideName) {
    return res.status(400).json({ error: 'ride Parameter erforderlich.' });
  }

  try {
    const result = await db.execute({
      sql: `SELECT DISTINCT recorded_date FROM wait_times WHERE park_id = ? AND ride_name = ? ORDER BY recorded_date ASC`,
      args: [parkId, rideName]
    });
    res.json({ days: result.rows.map(r => r.recorded_date) });
  } catch (err) {
    console.error('Fehler in /api/ride-days:', err.message);
    res.status(500).json({ error: 'Serverfehler.' });
  }
});

app.get('/api/ride-history', async (req, res) => {
  const parkId = req.query.park || '56';
  const rideName = req.query.ride;
  const date = req.query.date;
  const days = Math.min(parseInt(req.query.days, 10) || 30, 90);

  if (!rideName) {
    return res.status(400).json({ error: 'ride Parameter erforderlich.' });
  }

  try {
    if (date) {
      const result = await db.execute({
        sql: `
          SELECT recorded_time, is_open, wait_time
          FROM wait_times
          WHERE park_id = ? AND ride_name = ? AND recorded_date = ?
          ORDER BY recorded_at ASC
        `,
        args: [parkId, rideName, date]
      });

      res.json({
        mode: 'single-day',
        date,
        points: result.rows
      });

    } else {
      const cutoff = new Date();
      cutoff.setDate(cutoff.getDate() - days);
      const cutoffDate = cutoff.toLocaleDateString('sv-SE', { timeZone: 'Europe/Berlin' });

      const result = await db.execute({
        sql: `
          SELECT
            recorded_time,
            AVG(CASE WHEN is_open = 1 THEN wait_time ELSE NULL END) as avg_wait,
            COUNT(CASE WHEN is_open = 1 THEN 1 ELSE NULL END) as sample_count
          FROM wait_times
          WHERE park_id = ? AND ride_name = ? AND recorded_date >= ?
          GROUP BY recorded_time
          ORDER BY recorded_time ASC
        `,
        args: [parkId, rideName, cutoffDate]
      });

      let bestSlot = null;
      result.rows.forEach(row => {
        if (row.avg_wait === null || row.sample_count < 3) return;
        if (!bestSlot || row.avg_wait < bestSlot.avg_wait) {
          bestSlot = { time: row.recorded_time, avgWait: row.avg_wait, sampleCount: row.sample_count };
        }
      });

      res.json({
        mode: 'averaged',
        days,
        points: result.rows,
        recommendation: bestSlot
      });
    }

  } catch (err) {
    console.error('Fehler in /api/ride-history:', err.message);
    res.status(500).json({ error: 'Serverfehler.' });
  }
});

app.get('/api/weekday-stats', async (req, res) => {
  const parkId = req.query.park || '56';
  const days = Math.min(parseInt(req.query.days, 10) || 90, 180);

  try {
    const cutoff = new Date();
    cutoff.setDate(cutoff.getDate() - days);
    const cutoffDate = cutoff.toLocaleDateString('sv-SE', { timeZone: 'Europe/Berlin' });

    const result = await db.execute({
      sql: `
        SELECT
          weekday,
          AVG(CASE WHEN is_open = 1 THEN wait_time ELSE NULL END) as avg_wait,
          COUNT(DISTINCT recorded_date) as days_counted
        FROM wait_times
        WHERE park_id = ? AND recorded_date >= ?
        GROUP BY weekday
        ORDER BY weekday ASC
      `,
      args: [parkId, cutoffDate]
    });

    res.json({ weekdayStats: result.rows });

  } catch (err) {
    console.error('Fehler in /api/weekday-stats:', err.message);
    res.status(500).json({ error: 'Serverfehler.' });
  }
});

app.get('/api/weather-correlation', async (req, res) => {
  const parkId = req.query.park || '56';
  const rideName = req.query.ride;
  const days = Math.min(parseInt(req.query.days, 10) || 90, 180);

  try {
    const cutoff = new Date();
    cutoff.setDate(cutoff.getDate() - days);
    const cutoffDate = cutoff.toLocaleDateString('sv-SE', { timeZone: 'Europe/Berlin' });

    let sql = `
      SELECT
        CASE
          WHEN weather_code IS NULL THEN 'unbekannt'
          WHEN weather_code = 0 THEN 'klar'
          WHEN weather_code BETWEEN 1 AND 3 THEN 'bewölkt'
          WHEN weather_code BETWEEN 45 AND 48 THEN 'nebel'
          WHEN weather_code BETWEEN 51 AND 67 THEN 'regen'
          WHEN weather_code BETWEEN 71 AND 77 THEN 'schnee'
          WHEN weather_code BETWEEN 80 AND 82 THEN 'schauer'
          WHEN weather_code BETWEEN 95 AND 99 THEN 'gewitter'
          ELSE 'sonstiges'
        END as weather_category,
        AVG(CASE WHEN is_open = 1 THEN wait_time ELSE NULL END) as avg_wait,
        COUNT(DISTINCT recorded_date) as days_counted,
        AVG(temperature) as avg_temperature
      FROM wait_times
      WHERE park_id = ? AND recorded_date >= ? AND weather_code IS NOT NULL
    `;
    const args = [parkId, cutoffDate];

    if (rideName) {
      sql += ` AND ride_name = ?`;
      args.push(rideName);
    }

    sql += ` GROUP BY weather_category ORDER BY avg_wait DESC`;

    const result = await db.execute({ sql, args });
    res.json({ weatherCorrelation: result.rows });

  } catch (err) {
    console.error('Fehler in /api/weather-correlation:', err.message);
    res.status(500).json({ error: 'Serverfehler.' });
  }
});

app.get('/api/holiday-correlation', async (req, res) => {
  const parkId = req.query.park || '56';
  const rideName = req.query.ride;
  const days = Math.min(parseInt(req.query.days, 10) || 90, 180);

  try {
    const cutoff = new Date();
    cutoff.setDate(cutoff.getDate() - days);
    const cutoffDate = cutoff.toLocaleDateString('sv-SE', { timeZone: 'Europe/Berlin' });

    let baseWhere = `WHERE park_id = ? AND recorded_date >= ? AND is_school_holiday IS NOT NULL`;
    const baseArgs = [parkId, cutoffDate];
    if (rideName) baseWhere += ` AND ride_name = ?`;
    if (rideName) baseArgs.push(rideName);

    const result = await db.execute({
      sql: `
        SELECT
          is_school_holiday,
          is_public_holiday,
          AVG(CASE WHEN is_open = 1 THEN wait_time ELSE NULL END) as avg_wait,
          COUNT(DISTINCT recorded_date) as days_counted
        FROM wait_times
        ${baseWhere}
        GROUP BY is_school_holiday, is_public_holiday
      `,
      args: baseArgs
    });

    res.json({ holidayCorrelation: result.rows });

  } catch (err) {
    console.error('Fehler in /api/holiday-correlation:', err.message);
    res.status(500).json({ error: 'Serverfehler.' });
  }
});

app.get('/api/ride-correlations', async (req, res) => {
  const parkId = req.query.park || '56';
  const rideName = req.query.ride;
  const days = Math.min(parseInt(req.query.days, 10) || 30, 90);

  if (!rideName) {
    return res.status(400).json({ error: 'ride Parameter erforderlich.' });
  }

  try {
    const cutoff = new Date();
    cutoff.setDate(cutoff.getDate() - days);
    const cutoffDate = cutoff.toLocaleDateString('sv-SE', { timeZone: 'Europe/Berlin' });

    const targetResult = await db.execute({
      sql: `
        SELECT recorded_at, wait_time
        FROM wait_times
        WHERE park_id = ? AND ride_name = ? AND recorded_date >= ? AND is_open = 1
      `,
      args: [parkId, rideName, cutoffDate]
    });

    if (targetResult.rows.length < 10) {
      return res.json({ correlations: [], note: 'Noch nicht genug Daten für diese Attraktion.' });
    }

    const targetMap = new Map(targetResult.rows.map(r => [r.recorded_at, r.wait_time]));
    const targetAvg = targetResult.rows.reduce((sum, r) => sum + r.wait_time, 0) / targetResult.rows.length;

    const othersResult = await db.execute({
      sql: `
        SELECT ride_name, recorded_at, wait_time
        FROM wait_times
        WHERE park_id = ? AND ride_name != ? AND recorded_date >= ? AND is_open = 1
      `,
      args: [parkId, rideName, cutoffDate]
    });

    const grouped = {};
    othersResult.rows.forEach(row => {
      if (!grouped[row.ride_name]) grouped[row.ride_name] = [];
      grouped[row.ride_name].push(row);
    });

    const correlations = [];
    for (const [otherName, rows] of Object.entries(grouped)) {
      const otherAvg = rows.reduce((sum, r) => sum + r.wait_time, 0) / rows.length;
      let matchCount = 0;
      let totalCount = 0;

      rows.forEach(row => {
        if (!targetMap.has(row.recorded_at)) return;
        const targetVal = targetMap.get(row.recorded_at);
        const targetAboveAvg = targetVal > targetAvg;
        const otherAboveAvg = row.wait_time > otherAvg;
        if (targetAboveAvg === otherAboveAvg) matchCount++;
        totalCount++;
      });

      if (totalCount < 10) continue;

      const correlationScore = matchCount / totalCount;
      correlations.push({
        rideName: otherName,
        correlationScore: Math.round(correlationScore * 100) / 100,
        sharedDataPoints: totalCount
      });
    }

    correlations.sort((a, b) => b.correlationScore - a.correlationScore);

    res.json({ correlations: correlations.slice(0, 5) });

  } catch (err) {
    console.error('Fehler in /api/ride-correlations:', err.message);
    res.status(500).json({ error: 'Serverfehler.' });
  }
});

app.get('/api/smart-forecast', async (req, res) => {
  const parkId = req.query.park || '56';
  const rideName = req.query.ride;

  if (!rideName) {
    return res.status(400).json({ error: 'ride Parameter erforderlich.' });
  }

  try {
    const now = new Date();
    const today = now.toLocaleDateString('sv-SE', { timeZone: 'Europe/Berlin' });
    const weekday = new Date(now.toLocaleString('en-US', { timeZone: 'Europe/Berlin' })).getDay();
    const currentHour = parseInt(now.toLocaleTimeString('de-DE', { timeZone: 'Europe/Berlin', hour: '2-digit', hour12: false }), 10);

    if (holidayCache.lastFetched === 0) await refreshHolidayCache();
    const schoolHolidayInfo = getSchoolHolidayInfo(today);
    const weather = currentWeatherCache;

    const baseResult = await db.execute({
      sql: `
        SELECT AVG(wait_time) as avg_wait, COUNT(*) as sample_count
        FROM wait_times
        WHERE park_id = ? AND ride_name = ? AND weekday = ?
          AND CAST(substr(recorded_time, 1, 2) AS INTEGER) = ?
          AND is_open = 1
      `,
      args: [parkId, rideName, weekday, currentHour]
    });
    const base = baseResult.rows[0];

    const holidayResult = await db.execute({
      sql: `
        SELECT is_school_holiday, AVG(wait_time) as avg_wait
        FROM wait_times
        WHERE park_id = ? AND ride_name = ? AND is_open = 1 AND is_school_holiday IS NOT NULL
        GROUP BY is_school_holiday
      `,
      args: [parkId, rideName]
    });
    const holidayRows = holidayResult.rows;
    const holidayAvg = holidayRows.find(r => r.is_school_holiday === 1);
    const normalAvg = holidayRows.find(r => r.is_school_holiday === 0);
    let holidayFactor = 1;
    if (holidayAvg && normalAvg && normalAvg.avg_wait > 0) {
      holidayFactor = schoolHolidayInfo.isHoliday
        ? (holidayAvg.avg_wait / normalAvg.avg_wait)
        : 1;
    }

    let weatherFactor = 1;
    if (weather && weather.weatherCode !== null) {
      const isRainy = weather.weatherCode >= 51 && weather.weatherCode <= 82;
      const weatherResult = await db.execute({
        sql: `
          SELECT
            CASE WHEN weather_code BETWEEN 51 AND 82 THEN 1 ELSE 0 END as is_rainy,
            AVG(wait_time) as avg_wait
          FROM wait_times
          WHERE park_id = ? AND ride_name = ? AND is_open = 1 AND weather_code IS NOT NULL
          GROUP BY is_rainy
        `,
        args: [parkId, rideName]
      });
      const rainyAvg = weatherResult.rows.find(r => r.is_rainy === 1);
      const dryAvg = weatherResult.rows.find(r => r.is_rainy === 0);
      if (isRainy && rainyAvg && dryAvg && dryAvg.avg_wait > 0) {
        weatherFactor = rainyAvg.avg_wait / dryAvg.avg_wait;
      }
    }

    const historicalBase = base && base.avg_wait !== null ? base.avg_wait : null;
    const adjustedPrediction = historicalBase !== null
      ? Math.round(historicalBase * holidayFactor * weatherFactor)
      : null;

    res.json({
      prediction: adjustedPrediction,
      historicalBase: historicalBase !== null ? Math.round(historicalBase) : null,
      sampleCount: base ? base.sample_count : 0,
      factors: {
        isSchoolHoliday: schoolHolidayInfo.isHoliday,
        holidayCountries: schoolHolidayInfo.countries,
        holidayFactor: Math.round(holidayFactor * 100) / 100,
        currentWeather: weather,
        weatherFactor: Math.round(weatherFactor * 100) / 100
      },
      confidence: base && base.sample_count >= 5 ? 'hoch' : base && base.sample_count >= 2 ? 'mittel' : 'niedrig'
    });

  } catch (err) {
    console.error('Fehler in /api/smart-forecast:', err.message);
    res.status(500).json({ error: 'Serverfehler.' });
  }
});

// ---------- NEU: TAGES-PROGNOSE (kombiniert 30-Tage-Ø je Zeit-Slot + Wetter + Ferien) ----------
// Liefert für jeden 15-Minuten-Slot des heutigen Öffnungszeitraums eine
// vorhergesagte Wartezeit, basierend auf dem historischen Ø der letzten N Tage
// für genau diesen Wochentag+Uhrzeit-Slot (bevorzugt), mit Fallback auf den
// Ø aller Wochentage für diesen Slot falls zu wenig Daten. Das Ergebnis wird
// zusätzlich mit dem Wetter- und Ferienfaktor aus /api/smart-forecast skaliert,
// damit ALLE Faktoren (Wochentag, Uhrzeit, Wetter, Ferien) gemeinsam in die
// Prognose einfließen - für die komplette Tagesvorschau in den Graphen.
app.get('/api/day-forecast', async (req, res) => {
  const parkId = req.query.park || '56';
  const rideName = req.query.ride;
  const days = Math.min(parseInt(req.query.days, 10) || 30, 90);

  if (!rideName) {
    return res.status(400).json({ error: 'ride Parameter erforderlich.' });
  }

  try {
    const now = new Date();
    const today = now.toLocaleDateString('sv-SE', { timeZone: 'Europe/Berlin' });
    const weekday = new Date(now.toLocaleString('en-US', { timeZone: 'Europe/Berlin' })).getDay();

    const cutoff = new Date();
    cutoff.setDate(cutoff.getDate() - days);
    const cutoffDate = cutoff.toLocaleDateString('sv-SE', { timeZone: 'Europe/Berlin' });

    if (holidayCache.lastFetched === 0) await refreshHolidayCache();
    const schoolHolidayInfo = getSchoolHolidayInfo(today);
    const weather = currentWeatherCache;

    // 1) Ø Wartezeit je Uhrzeit-Slot, NUR für denselben Wochentag, letzte N Tage
    const sameWeekdayResult = await db.execute({
      sql: `
        SELECT
          recorded_time,
          AVG(CASE WHEN is_open = 1 THEN wait_time ELSE NULL END) as avg_wait,
          COUNT(CASE WHEN is_open = 1 THEN 1 ELSE NULL END) as sample_count
        FROM wait_times
        WHERE park_id = ? AND ride_name = ? AND recorded_date >= ? AND weekday = ?
        GROUP BY recorded_time
        ORDER BY recorded_time ASC
      `,
      args: [parkId, rideName, cutoffDate, weekday]
    });

    // 2) Fallback: Ø Wartezeit je Uhrzeit-Slot über ALLE Wochentage, letzte N Tage
    // (für Slots, wo der gleiche Wochentag zu wenig Datenpunkte hat)
    const allWeekdaysResult = await db.execute({
      sql: `
        SELECT
          recorded_time,
          AVG(CASE WHEN is_open = 1 THEN wait_time ELSE NULL END) as avg_wait,
          COUNT(CASE WHEN is_open = 1 THEN 1 ELSE NULL END) as sample_count
        FROM wait_times
        WHERE park_id = ? AND ride_name = ? AND recorded_date >= ?
        GROUP BY recorded_time
        ORDER BY recorded_time ASC
      `,
      args: [parkId, rideName, cutoffDate]
    });

    const sameWeekdayMap = new Map(sameWeekdayResult.rows.map(r => [r.recorded_time, r]));
    const allWeekdaysMap = new Map(allWeekdaysResult.rows.map(r => [r.recorded_time, r]));

    const MIN_SAMPLES_PREFERRED = 3;

    // Alle bekannten Zeit-Slots zusammenführen
    const allTimes = new Set([...sameWeekdayMap.keys(), ...allWeekdaysMap.keys()]);

    // Ferienfaktor bestimmen (wie in /api/smart-forecast)
    const holidayResult = await db.execute({
      sql: `
        SELECT is_school_holiday, AVG(wait_time) as avg_wait
        FROM wait_times
        WHERE park_id = ? AND ride_name = ? AND is_open = 1 AND is_school_holiday IS NOT NULL
        GROUP BY is_school_holiday
      `,
      args: [parkId, rideName]
    });
    const holidayRows = holidayResult.rows;
    const holidayAvgRow = holidayRows.find(r => r.is_school_holiday === 1);
    const normalAvgRow = holidayRows.find(r => r.is_school_holiday === 0);
    let holidayFactor = 1;
    if (holidayAvgRow && normalAvgRow && normalAvgRow.avg_wait > 0) {
      holidayFactor = schoolHolidayInfo.isHoliday
        ? (holidayAvgRow.avg_wait / normalAvgRow.avg_wait)
        : 1;
    }

    // Wetterfaktor bestimmen (wie in /api/smart-forecast)
    let weatherFactor = 1;
    if (weather && weather.weatherCode !== null) {
      const isRainy = weather.weatherCode >= 51 && weather.weatherCode <= 82;
      const weatherResult = await db.execute({
        sql: `
          SELECT
            CASE WHEN weather_code BETWEEN 51 AND 82 THEN 1 ELSE 0 END as is_rainy,
            AVG(wait_time) as avg_wait
          FROM wait_times
          WHERE park_id = ? AND ride_name = ? AND is_open = 1 AND weather_code IS NOT NULL
          GROUP BY is_rainy
        `,
        args: [parkId, rideName]
      });
      const rainyAvg = weatherResult.rows.find(r => r.is_rainy === 1);
      const dryAvg = weatherResult.rows.find(r => r.is_rainy === 0);
      if (isRainy && rainyAvg && dryAvg && dryAvg.avg_wait > 0) {
        weatherFactor = rainyAvg.avg_wait / dryAvg.avg_wait;
      }
    }

    const combinedFactor = holidayFactor * weatherFactor;

    const slots = [];
    for (const time of allTimes) {
      const sameRow = sameWeekdayMap.get(time);
      let baseAvg = null;
      let sampleCount = 0;
      let source = null;

      if (sameRow && sameRow.avg_wait !== null && sameRow.sample_count >= MIN_SAMPLES_PREFERRED) {
        baseAvg = sameRow.avg_wait;
        sampleCount = sameRow.sample_count;
        source = 'weekday';
      } else {
        const allRow = allWeekdaysMap.get(time);
        if (allRow && allRow.avg_wait !== null) {
          baseAvg = allRow.avg_wait;
          sampleCount = allRow.sample_count;
          source = 'all-days';
        }
      }

      if (baseAvg === null) continue;

      slots.push({
        time,
        baseAvg: Math.round(baseAvg * 10) / 10,
        forecast: Math.max(0, Math.round(baseAvg * combinedFactor)),
        sampleCount,
        source
      });
    }

    slots.sort((a, b) => a.time.localeCompare(b.time));

    res.json({
      slots,
      factors: {
        isSchoolHoliday: schoolHolidayInfo.isHoliday,
        holidayCountries: schoolHolidayInfo.countries,
        holidayFactor: Math.round(holidayFactor * 100) / 100,
        currentWeather: weather,
        weatherFactor: Math.round(weatherFactor * 100) / 100,
        combinedFactor: Math.round(combinedFactor * 100) / 100
      },
      basedOnDays: days,
      weekday
    });

  } catch (err) {
    console.error('Fehler in /api/day-forecast:', err.message);
    res.status(500).json({ error: 'Serverfehler.' });
  }
});

// ---------- HILFSFUNKTION: AUSFÄLLE AUS EINER TAGES-ZEITREIHE ERMITTELN ----------
// Nimmt eine chronologisch sortierte Liste von Messpunkten für EINEN Tag und
// EINE Attraktion und liefert die erkannten Ausfälle zurück. Regel: alles vor
// der ersten Öffnung des Tages ist "geplant zu" (späterer reguärer Öffnungs-
// zeitpunkt der Attraktion) und zählt NICHT als Ausfall. Jede Lücke danach,
// in der die Attraktion geschlossen ist (is_open=0 oder wait_time=0 während
// sie vorher offen war), gilt als Ausfall - bis sie wieder öffnet oder der
// Tag/die Messreihe endet.
function detectOutagesForDay(pointsChronological) {
  const outages = [];
  let firstOpenIndex = -1;

  for (let i = 0; i < pointsChronological.length; i++) {
    if (pointsChronological[i].is_open) { firstOpenIndex = i; break; }
  }
  if (firstOpenIndex === -1) return outages; // Attraktion war den ganzen Tag nie offen

  let outageStart = null;
  for (let i = firstOpenIndex; i < pointsChronological.length; i++) {
    const p = pointsChronological[i];
    const isDown = !p.is_open;

    if (isDown && outageStart === null) {
      outageStart = p;
    } else if (!isDown && outageStart !== null) {
      outages.push({
        startTime: outageStart.recorded_time,
        endTime: p.recorded_time,
        startedAt: outageStart.recorded_at,
        endedAt: p.recorded_at
      });
      outageStart = null;
    }
  }
  // Falls der Tag mit einem laufenden Ausfall endet (z.B. Attraktion bleibt
  // bis Parkschluss zu), zählt das bis zum letzten Messpunkt des Tages
  if (outageStart !== null) {
    const last = pointsChronological[pointsChronological.length - 1];
    outages.push({
      startTime: outageStart.recorded_time,
      endTime: last.recorded_time,
      startedAt: outageStart.recorded_at,
      endedAt: last.recorded_at,
      ongoing: true
    });
  }

  return { outages, firstOpenTime: pointsChronological[firstOpenIndex].recorded_time, lastTime: pointsChronological[pointsChronological.length - 1].recorded_time };
}

// ---------- AUSFÄLLE FÜR EINEN EINZELNEN TAG (für Graphen-Overlay) ----------
// Liefert die erkannten Ausfall-Zeitfenster für eine Attraktion an einem
// bestimmten Tag - wird vom Frontend genutzt, um rote Balken in den Live-
// und Einzeltag-Graphen einzuzeichnen.
app.get('/api/ride-outages', async (req, res) => {
  const parkId = req.query.park || '56';
  const rideName = req.query.ride;
  const date = req.query.date; // YYYY-MM-DD, optional - default heute

  if (!rideName) {
    return res.status(400).json({ error: 'ride Parameter erforderlich.' });
  }

  try {
    const targetDate = date || new Date().toLocaleDateString('sv-SE', { timeZone: 'Europe/Berlin' });

    const result = await db.execute({
      sql: `
        SELECT recorded_time, recorded_at, is_open
        FROM wait_times
        WHERE park_id = ? AND ride_name = ? AND recorded_date = ?
        ORDER BY recorded_at ASC
      `,
      args: [parkId, rideName, targetDate]
    });

    if (result.rows.length === 0) {
      return res.json({ date: targetDate, outages: [], firstOpenTime: null });
    }

    const { outages, firstOpenTime } = detectOutagesForDay(result.rows);

    res.json({ date: targetDate, outages, firstOpenTime });

  } catch (err) {
    console.error('Fehler in /api/ride-outages:', err.message);
    res.status(500).json({ error: 'Serverfehler.' });
  }
});

// ---------- ZUVERLÄSSIGKEITS-STATISTIK EINER ATTRAKTION ----------
// Berechnet über einen Zeitraum: Betriebsquote (% der reguären Öffnungszeit,
// in der die Attraktion tatsächlich lief), Anzahl Ausfälle, Ø Ausfalldauer,
// und die Uhrzeit(-Stunde), zu der Ausfälle am häufigsten beginnen. "Reguläre
// Öffnungszeit" = ab der ERSTEN Öffnung des jeweiligen Tages bis zur letzten
// Messung des Tages (Parkschluss) - so zählt ein späterer reguärer Start
// (z.B. 11 statt 9 Uhr) NICHT als Ausfall, siehe detectOutagesForDay().
app.get('/api/ride-reliability', async (req, res) => {
  const parkId = req.query.park || '56';
  const rideName = req.query.ride;
  const days = Math.min(parseInt(req.query.days, 10) || 30, 90);

  if (!rideName) {
    return res.status(400).json({ error: 'ride Parameter erforderlich.' });
  }

  try {
    const cutoff = new Date();
    cutoff.setDate(cutoff.getDate() - days);
    const cutoffDate = cutoff.toLocaleDateString('sv-SE', { timeZone: 'Europe/Berlin' });

    const result = await db.execute({
      sql: `
        SELECT recorded_date, recorded_time, recorded_at, is_open
        FROM wait_times
        WHERE park_id = ? AND ride_name = ? AND recorded_date >= ?
        ORDER BY recorded_date ASC, recorded_at ASC
      `,
      args: [parkId, rideName, cutoffDate]
    });

    if (result.rows.length === 0) {
      return res.json({
        uptimePercent: null, totalOutages: 0, avgOutageDurationMinutes: null,
        mostCommonOutageHour: null, daysAnalyzed: 0
      });
    }

    // Nach Tagen gruppieren
    const byDay = {};
    result.rows.forEach(row => {
      if (!byDay[row.recorded_date]) byDay[row.recorded_date] = [];
      byDay[row.recorded_date].push(row);
    });

    let totalOpenMinutes = 0;      // Summe der regulären Öffnungszeit (ab erster Öffnung bis Tagesende)
    let totalDownMinutes = 0;      // Summe der tatsächlichen Ausfallzeit innerhalb dieser Öffnungszeit
    let allOutages = [];           // alle erkannten Ausfälle über alle Tage, für Ø-Dauer & häufigste Uhrzeit
    let daysWithOpeningData = 0;

    // Schätzt die Dauer zwischen zwei Messpunkten in Minuten (üblich: 15 Min
    // Messintervall) - genutzt um Lücken zwischen Messpunkten zu überbrücken
    function minutesBetween(timeA, timeB) {
      const [ha, ma] = timeA.split(':').map(Number);
      const [hb, mb] = timeB.split(':').map(Number);
      return (hb * 60 + mb) - (ha * 60 + ma);
    }

    Object.values(byDay).forEach(dayPoints => {
      const { outages, firstOpenTime, lastTime } = detectOutagesForDay(dayPoints);
      if (!firstOpenTime) return; // Attraktion war diesen Tag nie offen -> nicht in Betriebszeit-Berechnung einbeziehen

      daysWithOpeningData++;
      const openWindowMinutes = Math.max(0, minutesBetween(firstOpenTime, lastTime));
      totalOpenMinutes += openWindowMinutes;

      outages.forEach(o => {
        const dur = Math.max(0, minutesBetween(o.startTime, o.endTime));
        totalDownMinutes += dur;
        allOutages.push({ ...o, durationMinutes: dur });
      });
    });

    const uptimePercent = totalOpenMinutes > 0
      ? Math.round(((totalOpenMinutes - totalDownMinutes) / totalOpenMinutes) * 1000) / 10
      : null;

    const avgOutageDurationMinutes = allOutages.length > 0
      ? Math.round(allOutages.reduce((sum, o) => sum + o.durationMinutes, 0) / allOutages.length)
      : null;

    // Häufigste Ausfall-Startstunde ermitteln (Modus über alle erkannten Ausfälle)
    let mostCommonOutageHour = null;
    if (allOutages.length > 0) {
      const hourCounts = {};
      allOutages.forEach(o => {
        const hour = parseInt(o.startTime.split(':')[0], 10);
        hourCounts[hour] = (hourCounts[hour] || 0) + 1;
      });
      let maxCount = 0;
      Object.entries(hourCounts).forEach(([hour, count]) => {
        if (count > maxCount) { maxCount = count; mostCommonOutageHour = parseInt(hour, 10); }
      });
    }

    res.json({
      uptimePercent,
      totalOutages: allOutages.length,
      avgOutageDurationMinutes,
      mostCommonOutageHour,
      daysAnalyzed: daysWithOpeningData
    });

  } catch (err) {
    console.error('Fehler in /api/ride-reliability:', err.message);
    res.status(500).json({ error: 'Serverfehler.' });
  }
});

// ---------- REGENPERIODEN (für blaue Overlay-Balken in den Graphen) ----------
// Wandelt die pro-Messpunkt gespeicherten Wettercodes eines Tages in
// zusammenhängende Regen-Zeitfenster um (Start/Ende), damit das Frontend
// diese als durchgezogene blaue Balken neben/unter der Wartezeit-Linie
// einzeichnen kann. "Regen" = WMO-Code 51-82 (Niesel, Regen, Schauer).
app.get('/api/rain-periods', async (req, res) => {
  const parkId = req.query.park || '56';
  const date = req.query.date;

  try {
    const targetDate = date || new Date().toLocaleDateString('sv-SE', { timeZone: 'Europe/Berlin' });

    // Wetterdaten sind pro Park+Zeitpunkt identisch über alle Attraktionen
    // gespeichert (siehe fetchAndSaveData) - daher reicht EINE beliebige
    // Attraktion als Quelle, um die Zeitreihe der Wettercodes zu bekommen.
    const result = await db.execute({
      sql: `
        SELECT recorded_time, recorded_at, weather_code
        FROM wait_times
        WHERE park_id = ? AND recorded_date = ? AND weather_code IS NOT NULL
        GROUP BY recorded_time
        ORDER BY recorded_at ASC
      `,
      args: [parkId, targetDate]
    });

    const periods = [];
    let rainStart = null;

    for (let i = 0; i < result.rows.length; i++) {
      const row = result.rows[i];
      const isRainy = row.weather_code >= 51 && row.weather_code <= 82;

      if (isRainy && rainStart === null) {
        rainStart = row;
      } else if (!isRainy && rainStart !== null) {
        periods.push({ startTime: rainStart.recorded_time, endTime: row.recorded_time });
        rainStart = null;
      }
    }
    if (rainStart !== null) {
      const last = result.rows[result.rows.length - 1];
      periods.push({ startTime: rainStart.recorded_time, endTime: last.recorded_time, ongoing: true });
    }

    res.json({ date: targetDate, periods });

  } catch (err) {
    console.error('Fehler in /api/rain-periods:', err.message);
    res.status(500).json({ error: 'Serverfehler.' });
  }
});

async function start() {
  try {
    await initDatabase();
    await fetchAndSaveData();

    app.listen(PORT, () => {
      console.log(`ParkPulse Server läuft auf Port ${PORT}`);
    });
  } catch (err) {
    console.error('❌ Fehler beim Start:', err.message);
    process.exit(1);
  }
}

start();
