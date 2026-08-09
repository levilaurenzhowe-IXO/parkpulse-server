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
// WICHTIG: URL und Token kommen aus Umgebungsvariablen (Render Dashboard),
// niemals hier fest eintragen -> sonst landen sie auf GitHub!
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

// ---------- WETTER-KOORDINATEN (Phantasialand, Brühl) ----------
const PARK_LATITUDE = 50.801472;
const PARK_LONGITUDE = 6.876355;

// Aktuell gecachtes Wetter, wird alle 15 Min zusammen mit den Wartezeiten
// aktualisiert (spart unnötige API-Calls zwischen den Fetch-Zyklen)
let currentWeatherCache = null;
let currentSchoolHolidayCache = null; // wird 1x täglich aktualisiert

// ---------- DATENBANK-SCHEMA ANLEGEN (einmalig beim Start) ----------
async function initDatabase() {
  // Eine Zeile pro Messpunkt pro Attraktion. Das erlaubt uns später beliebige
  // Auswertungen nach Datum, Wochentag, Uhrzeit, pro Attraktion etc.
  // Wetter- und Ferienspalten sind bewusst NULLABLE, damit alte, bereits
  // gespeicherte Zeilen (ohne diese Daten) nicht brechen - ALTER TABLE fügt
  // sie nachträglich hinzu, falls die Tabelle schon existiert.
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

  // Nachträgliches Hinzufügen neuer Spalten für bereits existierende
  // Tabellen (ALTER TABLE schlägt fehl, wenn die Spalte schon existiert -
  // das wird bewusst abgefangen und ignoriert)
  const newColumns = [
    { name: 'temperature', type: 'REAL' },       // °C
    { name: 'precipitation', type: 'REAL' },     // mm
    { name: 'weather_code', type: 'INTEGER' },   // WMO-Wettercode (0=klar, 61=Regen, etc.)
    { name: 'is_school_holiday', type: 'INTEGER' }, // 0/1 - JA in mind. einer Region (DE-NW/NL/BE/FR/LU)
    { name: 'holiday_countries', type: 'TEXT' },  // z.B. "Niederlande,Belgien" - welche Länder genau
    { name: 'is_public_holiday', type: 'INTEGER' } // 0/1 - Feiertag in mind. einer Region
  ];
  for (const col of newColumns) {
    try {
      await db.execute(`ALTER TABLE wait_times ADD COLUMN ${col.name} ${col.type}`);
    } catch (err) {
      // Spalte existiert bereits - kein Problem, einfach überspringen
    }
  }

  // Indexe für schnelle Abfragen (nach Park+Datum, und nach Attraktion)
  await db.execute(`CREATE INDEX IF NOT EXISTS idx_park_date ON wait_times (park_id, recorded_date)`);
  await db.execute(`CREATE INDEX IF NOT EXISTS idx_ride_name ON wait_times (park_id, ride_name)`);

  // Tabelle für ausgeblendete Attraktionen (pro Park)
  // HINWEIS: Wird aktuell NICHT vom Frontend genutzt - Ausblenden läuft bewusst
  // rein lokal über localStorage im Browser (siehe index.html), damit jedes
  // Familienmitglied eigene Einstellungen hat. Tabelle bleibt für mögliche
  // spätere Server-Funktionen (z.B. "häufig ausgeblendet") bestehen.
  await db.execute(`
    CREATE TABLE IF NOT EXISTS hidden_rides (
      park_id TEXT NOT NULL,
      ride_name TEXT NOT NULL,
      PRIMARY KEY (park_id, ride_name)
    )
  `);

  console.log('✅ Datenbank-Schema bereit.');
}

// ---------- WETTER ABRUFEN (Open-Meteo, kostenlos, kein API-Key nötig) ----------
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

// ---------- FERIEN & FEIERTAGE (OpenHolidays API, kostenlos, kein Key nötig) ----------
// Deckt Schulferien UND Feiertage für Deutschland (NRW) sowie die wichtigsten
// Nachbarländer ab, aus denen Phantasialand-Besucher kommen: Niederlande,
// Belgien, Frankreich, Luxemburg. Wird 1x täglich aktualisiert und im
// Arbeitsspeicher gecacht (Ferienzeiträume ändern sich nicht stündlich).
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
    // API liefert direkt ein Array (kein Wrapper-Objekt)
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
  future.setFullYear(future.getFullYear() + 1); // 1 Jahr im Voraus abdecken
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

// Prüft für ein Datum, ob es in MINDESTENS EINER der Regionen Schulferien sind,
// und liefert gleich mit, in welchen Ländern genau (für spätere Auswertung)
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

// Cache 1x täglich auffrischen (Ferientermine ändern sich nicht kurzfristig)
cron.schedule('0 3 * * *', () => {
  refreshHolidayCache();
});


// ---------- DATEN ABRUFEN UND SPEICHERN ----------
async function fetchAndSaveData() {
  console.log(`[${new Date().toISOString()}] Starte Datenabruf für alle Parks...`);

  // Wetter einmal pro Zyklus abrufen (gilt für alle Parks gleichermaßen als
  // Näherung, da alle PARKS aktuell in ähnlicher Klimazone liegen; exakt für
  // Phantasialand berechnet über PARK_LATITUDE/LONGITUDE)
  currentWeatherCache = await fetchCurrentWeather();

  const now = new Date();
  const recordedDate = now.toLocaleDateString('sv-SE', { timeZone: 'Europe/Berlin' });
  const recordedTime = now.toLocaleTimeString('de-DE', { timeZone: 'Europe/Berlin', hour: '2-digit', minute: '2-digit' });
  const weekday = new Date(now.toLocaleString('en-US', { timeZone: 'Europe/Berlin' })).getDay();

  // Ferien-Cache initial befüllen, falls noch leer (z.B. direkt nach Serverstart)
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

      // Alle Attraktionen dieses Parks in einem Batch einfügen (effizienter als einzeln),
      // jetzt inklusive Wetter- und Ferien-Kontext für spätere Korrelationsanalysen
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

// Alle 15 Minuten automatisch neue Daten holen
cron.schedule('*/15 * * * *', () => {
  fetchAndSaveData();
});

// --- API ENDPUNKTE ---

// Health-Check: Wird von einem externen kostenlosen Dienst (z.B. cron-job.org)
// alle paar Minuten aufgerufen, damit Render den Server nie in den Ruhemodus
// schickt. So läuft der interne 15-Minuten-Cronjob (oben) durchgehend weiter,
// auch wenn gerade niemand die App öffnet.
app.get('/health', (req, res) => {
  res.status(200).json({
    status: 'ok',
    timestamp: new Date().toISOString(),
    lastDataFetch: lastFetchTimestamp ? new Date(lastFetchTimestamp).toISOString() : null
  });
});

// Liefert alles, was das Frontend für die Wetter-/Ferien-Übersichtsbox und
// den Kalender braucht: aktuelles Wetter, heutiger Ferien-/Feiertagsstatus je
// Region, sowie die vollständigen Zeiträume für den Kalender (nächste Monate).
app.get('/api/context', async (req, res) => {
  try {
    if (holidayCache.lastFetched === 0) {
      await refreshHolidayCache();
    }

    const now = new Date();
    const today = now.toLocaleDateString('sv-SE', { timeZone: 'Europe/Berlin' });

    const schoolHolidayInfo = getSchoolHolidayInfo(today);
    const publicHolidayInfo = getPublicHolidayInfo(today);

    // Aktives Wetter: falls der Cache noch leer ist (z.B. direkt nach Start),
    // einmalig live nachladen, statt "null" zurückzugeben
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
      // Kompletter Kalender für die Anzeige: alle Zeiträume, die in den Cache
      // geladen sind (aktuell bis 1 Jahr im Voraus, siehe refreshHolidayCache)
      schoolHolidays: holidayCache.schoolHolidays,
      publicHolidays: holidayCache.publicHolidays,
      lastRefreshed: holidayCache.lastFetched ? new Date(holidayCache.lastFetched).toISOString() : null
    });

  } catch (err) {
    console.error('Fehler in /api/context:', err.message);
    res.status(500).json({ error: 'Serverfehler.' });
  }
});

// Aktuelle Live-Daten + heutiger Verlauf (ab Parköffnung, also ab dem ersten
// Messpunkt von heute) für einen Park
app.get('/api/park', async (req, res) => {
  const parkId = req.query.park || '56';

  try {
    if (Date.now() - lastFetchTimestamp > 10 * 60 * 1000) {
      console.log('Server war inaktiv, hole frische Daten...');
      await fetchAndSaveData();
    }

    const today = new Date().toLocaleDateString('sv-SE', { timeZone: 'Europe/Berlin' });

    // Alle heutigen Messpunkte holen, chronologisch sortiert
    const result = await db.execute({
      sql: `SELECT * FROM wait_times WHERE park_id = ? AND recorded_date = ? ORDER BY recorded_at ASC`,
      args: [parkId, today]
    });

    // Ausgeblendete Attraktionen holen
    const hiddenResult = await db.execute({
      sql: `SELECT ride_name FROM hidden_rides WHERE park_id = ?`,
      args: [parkId]
    });
    const hiddenNames = new Set(hiddenResult.rows.map(r => r.ride_name));

    // Rohdaten (eine Zeile pro Attraktion+Messpunkt) zurück in das alte
    // "history"-Format gruppieren, das das Frontend erwartet:
    // [{ time, timestamp, rides: [{name, isOpen, waitTime}, ...] }, ...]
    const grouped = {};
    for (const row of result.rows) {
      if (hiddenNames.has(row.ride_name)) continue; // ausgeblendete Fahrgeschäfte überspringen

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

// Attraktion ausblenden / wieder einblenden
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

// Wochentags-/Uhrzeit-Statistik pro Attraktion: durchschnittliche Wartezeit
// gruppiert nach Wochentag und Stunde -> Basis für Empfehlungen
app.get('/api/stats', async (req, res) => {
  const parkId = req.query.park || '56';
  const rideName = req.query.ride; // optional: nur eine bestimmte Attraktion

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

// Liste aller bekannten Attraktionsnamen eines Parks (für Ausblenden-UI)
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

// ---------- TAGES-STATISTIKEN (Legacy, evtl. später wiederverwendbar) ----------
//  - dailySummary: pro vergangenem Tag -> Ø Wartezeit, max. gleichzeitige "Besucherlast"
//  - perRideDaily: pro Tag UND pro Attraktion -> Ø Wartezeit (für die Tabelle)
//  - averageDay: alle Tage nach Uhrzeit gemittelt -> "so sieht ein durchschnittlicher Tag aus"
app.get('/api/daily-stats', async (req, res) => {
  const parkId = req.query.park || '56';
  const days = Math.min(parseInt(req.query.days, 10) || 30, 90); // max. 90 Tage abfragen

  try {
    // Älteste noch relevante Datumsgrenze berechnen (heute - X Tage)
    const cutoff = new Date();
    cutoff.setDate(cutoff.getDate() - days);
    const cutoffDate = cutoff.toLocaleDateString('sv-SE', { timeZone: 'Europe/Berlin' });

    // 1) Pro Tag: Durchschnittliche Wartezeit über alle offenen Attraktionen
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

    // 2) Pro Tag UND pro Attraktion: Durchschnittliche Wartezeit (für die Tabelle)
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

    // 3) "Durchschnittstag": alle Tage nach Uhrzeit (HH:MM) gemittelt, über alle
    // offenen Attraktionen -> zeigt den typischen Tagesverlauf unabhängig vom Datum
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

    // 4) Durchschnittstag PRO Attraktion (für den Fall, dass wir das später pro
    // Attraktion einzeln anzeigen wollen -> schon mitgeliefert, kostet kaum mehr)
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

// ---------- NEU: ATTRAKTIONS-BASIERTE STATISTIK ----------
// (für den neu gestalteten Statistik-Tab: Attraktion wählen -> Kalender -> Tagesverlauf,
// plus gemittelter Verlauf über einen Zeitraum mit Uhrzeit-Empfehlung)

// Liefert alle Tage, an denen für eine bestimmte Attraktion Daten existieren
// (für den Kalender im Frontend, damit nur wirklich vorhandene Tage anklickbar sind)
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

// Liefert den Wartezeit-Verlauf für eine Attraktion.
// Modus 1 (date-Parameter gesetzt): Verlauf genau dieses einen Tages.
// Modus 2 (kein date, aber days-Parameter): Über den Zeitraum gemittelter
// Verlauf nach Uhrzeit, PLUS eine automatische Empfehlung für die beste
// Besuchszeit (Uhrzeit mit der niedrigsten Ø Wartezeit, min. 3 Datenpunkte).
app.get('/api/ride-history', async (req, res) => {
  const parkId = req.query.park || '56';
  const rideName = req.query.ride;
  const date = req.query.date; // z.B. "2026-08-07" - optional
  const days = Math.min(parseInt(req.query.days, 10) || 30, 90);

  if (!rideName) {
    return res.status(400).json({ error: 'ride Parameter erforderlich.' });
  }

  try {
    if (date) {
      // Modus 1: Einzelner Tag, chronologischer Verlauf
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
      // Modus 2: Über Zeitraum gemittelt, nach Uhrzeit gruppiert
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

      // Beste Uhrzeit ermitteln: niedrigste Ø Wartezeit, aber nur Zeitpunkte
      // mit mindestens 3 Messungen berücksichtigen (sonst zu unzuverlässig)
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

// Durchschnittliche Wartezeit gruppiert nach Wochentag (über alle Attraktionen
// und alle Tage im gewählten Zeitraum) -> Basis für den Wochentags-Vergleichsgraf
// im Statistik-Tab. Zeigt z.B. "Dienstags ist im Schnitt am leersten".
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

// ---------- WETTER-KORRELATION ----------
// Gruppiert Ø Wartezeit nach Wettercode-Kategorie (klar/bewölkt/regen/etc.)
// -> zeigt, wie stark das Wetter die Wartezeiten tatsächlich beeinflusst
app.get('/api/weather-correlation', async (req, res) => {
  const parkId = req.query.park || '56';
  const rideName = req.query.ride; // optional: nur eine Attraktion
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

// ---------- FERIEN-KORRELATION ----------
// Vergleicht Ø Wartezeit an Schulferien-/Feiertagen vs. normalen Tagen
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

// ---------- ATTRAKTIONS-KORRELATIONEN ----------
// Findet Attraktionen, deren Wartezeiten sich ähnlich verhalten (gemeinsame
// Auslastungsspitzen) - nützlich für Ausweich-Empfehlungen in Echtzeit.
// Nutzt eine vereinfachte Korrelation: für jeden gemeinsamen Zeitpunkt wird
// verglichen, ob beide Attraktionen relativ zu ihrem eigenen Ø-Wert gleich-
// zeitig über- oder unterdurchschnittlich ausgelastet waren.
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

    // Alle Messpunkte der Ziel-Attraktion holen
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

    // Alle anderen Attraktionen im selben Zeitraum holen
    const othersResult = await db.execute({
      sql: `
        SELECT ride_name, recorded_at, wait_time
        FROM wait_times
        WHERE park_id = ? AND ride_name != ? AND recorded_date >= ? AND is_open = 1
      `,
      args: [parkId, rideName, cutoffDate]
    });

    // Pro andere Attraktion: gemeinsame Zeitpunkte finden und einfache
    // Korrelation berechnen (übereinstimmende Richtung relativ zum Ø-Wert)
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

      if (totalCount < 10) continue; // zu wenig gemeinsame Datenpunkte

      const correlationScore = matchCount / totalCount; // 0.5 = keine Korrelation, 1.0 = perfekt gleichläufig
      correlations.push({
        rideName: otherName,
        correlationScore: Math.round(correlationScore * 100) / 100,
        sharedDataPoints: totalCount
      });
    }

    // Nach Korrelationsstärke sortieren (am stärksten gleichläufig zuerst)
    correlations.sort((a, b) => b.correlationScore - a.correlationScore);

    res.json({ correlations: correlations.slice(0, 5) }); // Top 5 reichen

  } catch (err) {
    console.error('Fehler in /api/ride-correlations:', err.message);
    res.status(500).json({ error: 'Serverfehler.' });
  }
});

// ---------- KOMBINIERTE SMARTE PROGNOSE ----------
// Kombiniert Wochentag + aktuelles Wetter + Ferienstatus + Live-Trend zu einer
// Vorhersage für die nächsten Stunden. Kein echtes ML-Modell, sondern eine
// nachvollziehbare, gewichtete Kombination der einzelnen Faktoren - für ein
// privates Projekt der richtige Kompromiss zwischen Aufwand und Nutzen.
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

    // Basis: historischer Durchschnitt für diesen Wochentag + diese Uhrzeit
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

    // Anpassungsfaktor durch Ferien (Vergleich Ferien- vs. Nicht-Ferientage
    // für diese Attraktion, falls genug Daten vorhanden)
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

    // Anpassungsfaktor durch aktuelles Wetter
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

// ---------- SERVER START ----------
async function start() {
  try {
    await initDatabase();
    await fetchAndSaveData(); // sofort beim Start erste Daten holen

    app.listen(PORT, () => {
      console.log(`ParkPulse Server läuft auf Port ${PORT}`);
    });
  } catch (err) {
    console.error('❌ Fehler beim Start:', err.message);
    process.exit(1);
  }
}

start();
