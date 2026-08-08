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

// ---------- DATENBANK-SCHEMA ANLEGEN (einmalig beim Start) ----------
async function initDatabase() {
  // Eine Zeile pro Messpunkt pro Attraktion. Das erlaubt uns später beliebige
  // Auswertungen nach Datum, Wochentag, Uhrzeit, pro Attraktion etc.
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

// ---------- DATEN ABRUFEN UND SPEICHERN ----------
async function fetchAndSaveData() {
  console.log(`[${new Date().toISOString()}] Starte Datenabruf für alle Parks...`);

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

      const now = new Date();
      // Deutsche Zeitzone für Datum/Uhrzeit/Wochentag (wichtig für Park in Deutschland)
      const recordedDate = now.toLocaleDateString('sv-SE', { timeZone: 'Europe/Berlin' }); // YYYY-MM-DD
      const recordedTime = now.toLocaleTimeString('de-DE', { timeZone: 'Europe/Berlin', hour: '2-digit', minute: '2-digit' });
      const weekday = new Date(now.toLocaleString('en-US', { timeZone: 'Europe/Berlin' })).getDay(); // 0=So, 1=Mo, ...

      // Alle Attraktionen dieses Parks in einem Batch einfügen (effizienter als einzeln)
      const statements = rides.map(r => ({
        sql: `INSERT INTO wait_times
              (park_id, ride_id, ride_name, is_open, wait_time, recorded_at, recorded_date, recorded_time, weekday)
              VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        args: [
          park.id,
          String(r.id || ''),
          r.name,
          r.is_open ? 1 : 0,
          r.wait_time || 0,
          Date.now(),
          recordedDate,
          recordedTime,
          weekday
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
