const express = require('express');
const cors = require('cors');
const cron = require('node-cron');
// Kein node-fetch mehr nötig: Node.js 18+ bringt ein natives, globales
// fetch() mit (stabil seit Node 21), das exakt dieselbe Fetch-API wie im
// Browser bereitstellt. Der Import entfällt komplett, der restliche Code
// bleibt unverändert, da überall bereits die Standard-fetch()-Syntax genutzt
// wird. Setzt Node >= 18 voraus (siehe "engines" in package.json).
const { createClient } = require('@libsql/client');

const app = express();
const PORT = process.env.PORT || 3000;

app.use(cors());
app.use(express.json());

// ---------- FETCH MIT TIMEOUT (gegen hängende externe Requests) ----------
// Alle externen APIs (queue-times.com, Open-Meteo, OpenHolidaysAPI,
// wartezeiten.app) wurden bisher OHNE jeden Timeout aufgerufen - hing eine
// dieser Fremdseiten mal (langsame Antwort, Netzwerkproblem, Rate-Limiting),
// konnte das den gesamten Speichervorgang bzw. nachfolgende Anfragen spürbar
// verzögern, ohne dass ein Fehler sichtbar wurde. fetchWithTimeout bricht
// nach timeoutMs automatisch ab und wirft einen klaren Fehler, der von den
// bestehenden try/catch-Blöcken wie gewohnt aufgefangen wird.
async function fetchWithTimeout(url, options = {}, timeoutMs = 10000) {
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), timeoutMs);
  try {
    return await fetch(url, { ...options, signal: controller.signal });
  } finally {
    clearTimeout(timeoutId);
  }
}

// ---------- TURSO DATENBANK VERBINDUNG ----------
const db = createClient({
  url: process.env.TURSO_DATABASE_URL,
  authToken: process.env.TURSO_AUTH_TOKEN,
});

// Ausschließlich Phantasialand - andere Parks wurden bewusst entfernt, da
// diese App exklusiv für Phantasialand gebaut ist. Vorher liefen Kings
// Island und Cedar Point unnötig im 5-Minuten-Speicherzyklus mit, obwohl sie
// nirgends in der App genutzt wurden - das kostete bei jedem Zyklus unnötig
// Zeit (2 zusätzliche externe API-Calls) und DB-Speicherplatz.
const PARKS = [
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
    { name: 'is_public_holiday', type: 'INTEGER' },
    { name: 'is_complete_snapshot', type: 'INTEGER' } // 1 = mind. MIN_RIDES_FOR_CROWD_DATA Attraktionen wurden in diesem Messzyklus gefunden -> für Besucherzahlen-Grafen nutzbar
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

  // Speichert die für einen Tag ermittelten Park-Öffnungszeiten, damit wir
  // (a) historisch nachvollziehen können, wann der Park wie lange offen war
  // (wichtig für Prognosen), und (b) im Live-Betrieb zuverlässig zwischen
  // "Attraktion ausgefallen" und "Park schon/noch geschlossen" unterscheiden
  // können. source markiert, ob die Zeiten erfolgreich von wartezeiten.app
  // gescraped wurden ('scraped'), aus einer alten erfolgreichen Abfrage
  // desselben Tages weiterverwendet wurden ('stale'), oder der harte
  // Notfall-Fallback 9-19 Uhr griff, weil gar nichts anderes verfügbar war
  // ('fallback') - siehe fetchParkOpeningHours().
  await db.execute(`
    CREATE TABLE IF NOT EXISTS park_opening_hours (
      park_id TEXT NOT NULL,
      date TEXT NOT NULL,
      open_time TEXT NOT NULL,
      close_time TEXT NOT NULL,
      source TEXT NOT NULL,
      updated_at INTEGER NOT NULL,
      PRIMARY KEY (park_id, date)
    )
  `);

  // Cache für die "intelligente" Ähnlichkeits-Prognose (siehe
  // computeSmartForecast()). Die Berechnung durchsucht potenziell sehr viele
  // historische Tage und darf bewusst etwas dauern - wird deshalb NICHT bei
  // jeder Anfrage neu gerechnet, sondern pro Attraktion gecacht und nur neu
  // berechnet, wenn sich die relevanten Einflussfaktoren (Wetter/Ferien/
  // bisheriger Tagesverlauf) seit der letzten Berechnung wirklich geändert
  // haben oder der Cache älter als 15 Minuten ist.
  await db.execute(`
    CREATE TABLE IF NOT EXISTS forecast_cache (
      park_id TEXT NOT NULL,
      ride_name TEXT NOT NULL,
      computed_at INTEGER NOT NULL,
      factors_fingerprint TEXT NOT NULL,
      result_json TEXT NOT NULL,
      PRIMARY KEY (park_id, ride_name)
    )
  `);

  console.log('✅ Datenbank-Schema bereit.');
}

async function fetchCurrentWeather() {
  try {
    const url = `https://api.open-meteo.com/v1/forecast?latitude=${PARK_LATITUDE}&longitude=${PARK_LONGITUDE}&current=temperature_2m,precipitation,weather_code&timezone=Europe%2FBerlin`;
    const res = await fetchWithTimeout(url, {}, 8000);
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

// ---------- ÖFFNUNGSZEITEN (von wartezeiten.app gescraped) ----------
// wartezeiten.app hat kein öffentliches JSON-API für Öffnungszeiten, daher
// wird die Live-Seite geladen und der Text "Von HH:MM bis HH:MM Uhr geöffnet"
// per Regex extrahiert. Das ist bewusst FEHLERTOLERANT aufgebaut, mit drei
// Stufen (siehe fetchParkOpeningHours):
//   1. Erfolgreich gescraped -> source='scraped', wird in der DB gespeichert
//   2. Scraping schlägt fehl, aber es gibt für HEUTE bereits einen früheren
//      erfolgreichen Scrape -> dieser wird weiterverwendet, source='stale'
//   3. Scraping schlägt fehl UND es gibt noch keinen erfolgreichen Scrape für
//      heute -> harter Notfall-Fallback 09:00-19:00 Uhr, source='fallback'
// In JEDEM Fall (auch bei Erfolg) wird zusätzlich global vermerkt, ob der
// letzte Scrape-VERSUCH geklappt hat (lastOpeningHoursScrapeError) - das
// Frontend zeigt dem Nutzer eine Warnung, wenn der Scraper zuletzt fehlschlug,
// auch wenn dank Fallback weiterhin Öffnungszeiten angezeigt werden.
const WARTEZEITEN_APP_URL = 'https://www.wartezeiten.app/phantasialand/';
const OPENING_HOURS_FALLBACK = { openTime: '09:00', closeTime: '19:00' };
let lastOpeningHoursScrapeError = null; // null = letzter Versuch war OK, sonst Fehlermeldung
let lastOpeningHoursScrapeAttempt = 0;

async function scrapeOpeningHoursFromWartezeitenApp() {
  // Ein unauffälliger, echter Browser-User-Agent statt eines Bot-erkennbaren
  // Strings (der vorherige "ParkPulse/1.0" wurde von wartezeiten.app mit
  // HTTP 403 blockiert). Zusätzlich ein paar Standard-Browser-Header, damit
  // die Anfrage nicht wie ein offensichtliches Skript aussieht.
  const res = await fetchWithTimeout(WARTEZEITEN_APP_URL, {
    headers: {
      'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
      'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
      'Accept-Language': 'de-DE,de;q=0.9'
    }
  }, 10000);
  if (!res.ok) throw new Error(`HTTP ${res.status} von wartezeiten.app`);

  const html = await res.text();

  // Geschlossen-Fall zuerst prüfen (z.B. Wartungstag außerhalb der Saison)
  if (/(heute|park)\s+geschlossen/i.test(html)) {
    return { openTime: null, closeTime: null, closed: true };
  }

  const match = html.match(/Von\s+(\d{1,2}):(\d{2})\s+bis\s+(\d{1,2}):(\d{2})\s+Uhr\s+geöffnet/i);
  if (!match) {
    throw new Error('Öffnungszeiten-Text nicht im HTML gefunden (Seitenlayout evtl. geändert)');
  }

  const openTime = `${match[1].padStart(2, '0')}:${match[2]}`;
  const closeTime = `${match[3].padStart(2, '0')}:${match[4]}`;
  return { openTime, closeTime, closed: false };
}

// Holt/aktualisiert die Öffnungszeiten für HEUTE mit dem oben beschriebenen
// dreistufigen Fallback. Wird stündlich per Cron aufgerufen UND einmalig
// beim Serverstart, damit auch nach einem Neustart sofort valide Zeiten da
// sind, statt bis zur nächsten vollen Stunde zu warten.
async function refreshOpeningHours(parkId = '56') {
  lastOpeningHoursScrapeAttempt = Date.now();
  const today = new Date().toLocaleDateString('sv-SE', { timeZone: 'Europe/Berlin' });

  try {
    const scraped = await scrapeOpeningHoursFromWartezeitenApp();
    lastOpeningHoursScrapeError = null; // Versuch war erfolgreich

    if (scraped.closed) {
      // Park heute laut Website komplett geschlossen (z.B. Wartungstag) -
      // trotzdem als "scraped" speichern, damit das Frontend das korrekt
      // von einem Ausfall unterscheiden kann (open_time=close_time markiert das)
      await db.execute({
        sql: `INSERT INTO park_opening_hours (park_id, date, open_time, close_time, source, updated_at)
              VALUES (?, ?, ?, ?, 'scraped', ?)
              ON CONFLICT(park_id, date) DO UPDATE SET open_time=excluded.open_time, close_time=excluded.close_time, source=excluded.source, updated_at=excluded.updated_at`,
        args: [parkId, today, '00:00', '00:00', Date.now()]
      });
      console.log(`🚪 Öffnungszeiten ${today}: Park laut wartezeiten.app heute geschlossen.`);
      return;
    }

    await db.execute({
      sql: `INSERT INTO park_opening_hours (park_id, date, open_time, close_time, source, updated_at)
            VALUES (?, ?, ?, ?, 'scraped', ?)
            ON CONFLICT(park_id, date) DO UPDATE SET open_time=excluded.open_time, close_time=excluded.close_time, source=excluded.source, updated_at=excluded.updated_at`,
      args: [parkId, today, scraped.openTime, scraped.closeTime, Date.now()]
    });
    console.log(`🕐 Öffnungszeiten ${today}: ${scraped.openTime}-${scraped.closeTime} Uhr (von wartezeiten.app).`);

  } catch (err) {
    lastOpeningHoursScrapeError = err.message;
    console.error('⚠️ Fehler beim Scrapen der Öffnungszeiten:', err.message);

    // Stufe 2: gibt es für HEUTE bereits einen früher erfolgreich gescrapeten
    // Eintrag? Dann diesen einfach stehen lassen (source bleibt 'scraped',
    // aber wir markieren den Fehlversuch separat über lastOpeningHoursScrapeError)
    const existing = await db.execute({
      sql: `SELECT * FROM park_opening_hours WHERE park_id = ? AND date = ?`,
      args: [parkId, today]
    });
    if (existing.rows.length > 0) {
      console.log(`↩️ Verwende weiterhin die zuletzt erfolgreich geladenen Öffnungszeiten für ${today}.`);
      return;
    }

    // Stufe 3: gar nichts vorhanden -> harter Notfall-Fallback 9-19 Uhr
    await db.execute({
      sql: `INSERT INTO park_opening_hours (park_id, date, open_time, close_time, source, updated_at)
            VALUES (?, ?, ?, ?, 'fallback', ?)
            ON CONFLICT(park_id, date) DO NOTHING`,
      args: [parkId, today, OPENING_HOURS_FALLBACK.openTime, OPENING_HOURS_FALLBACK.closeTime, Date.now()]
    });
    console.log(`🆘 Notfall-Fallback für ${today}: ${OPENING_HOURS_FALLBACK.openTime}-${OPENING_HOURS_FALLBACK.closeTime} Uhr.`);
  }
}

// Stündlicher Scrape (schont die fremde Seite, reagiert aber noch zeitnah
// auf spontane Verlängerungen/Verkürzungen der Öffnungszeit)
cron.schedule('0 * * * *', () => {
  refreshOpeningHours();
});

// Für den Ferienkalender-Tab wird "Deutschland" als EINE Zeile dargestellt,
// die zeigt, ob IRGENDEIN Bundesland gerade Ferien hat (Vereinigung aller 16
// Bundesländer), statt nur NRW zu zeigen. Dafür müssen alle 16 Bundesländer
// einzeln abgefragt werden (die API liefert Ferien pro Bundesland getrennt).
// Die einzelnen Bundesland-Namen bleiben dabei erhalten (fließen weiterhin in
// is_school_holiday/holiday_countries für die Attraktions-Prognose ein, siehe
// getSchoolHolidayInfo), werden aber im NEUEN Ferienkalender-Tab zu "Deutschland"
// zusammengefasst (siehe /api/holidays-overview weiter unten).
const GERMAN_STATE_CODES = [
  { code: 'DE-BW', name: 'Baden-Württemberg' },
  { code: 'DE-BY', name: 'Bayern' },
  { code: 'DE-BE', name: 'Berlin' },
  { code: 'DE-BB', name: 'Brandenburg' },
  { code: 'DE-HB', name: 'Bremen' },
  { code: 'DE-HH', name: 'Hamburg' },
  { code: 'DE-HE', name: 'Hessen' },
  { code: 'DE-MV', name: 'Mecklenburg-Vorpommern' },
  { code: 'DE-NI', name: 'Niedersachsen' },
  { code: 'DE-NW', name: 'Nordrhein-Westfalen' },
  { code: 'DE-RP', name: 'Rheinland-Pfalz' },
  { code: 'DE-SL', name: 'Saarland' },
  { code: 'DE-SN', name: 'Sachsen' },
  { code: 'DE-ST', name: 'Sachsen-Anhalt' },
  { code: 'DE-SH', name: 'Schleswig-Holstein' },
  { code: 'DE-TH', name: 'Thüringen' }
];

const HOLIDAY_REGIONS = [
  // NRW bleibt als bevorzugte Region für die Attraktions-Prognosen (Wetter/
  // Ferien-Einfluss auf Wartezeiten) bestehen, da der Park in NRW liegt und
  // NRW-Besucher den größten Einzelanteil ausmachen dürften.
  { country: 'DE', subdivision: 'DE-NW', label: 'Deutschland (NRW)' },
  { country: 'NL', subdivision: null, label: 'Niederlande' },
  { country: 'BE', subdivision: null, label: 'Belgien' },
  { country: 'FR', subdivision: null, label: 'Frankreich' },
  { country: 'LU', subdivision: null, label: 'Luxemburg' }
];

let holidayCache = { schoolHolidays: [], publicHolidays: [], germanUnion: [], lastFetched: 0 };

// Führt überlappende oder direkt aneinander angrenzende Datumsbereiche zu
// einem einzigen zusammenhängenden Zeitraum zusammen (Vereinigung/Union).
// Wird genutzt, um aus 16 einzelnen Bundesland-Ferienlisten EINE
// "Deutschland gesamt"-Zeitleiste zu bauen, in der jeder Tag, an dem
// mindestens ein Bundesland Ferien hat, als durchgehender grüner Balken
// erscheint - genau wie im Referenzdesign (schulferien.org).
function mergeDateRanges(ranges) {
  if (ranges.length === 0) return [];
  const sorted = [...ranges].sort((a, b) => a.startDate.localeCompare(b.startDate));
  const merged = [sorted[0]];

  for (let i = 1; i < sorted.length; i++) {
    const current = sorted[i];
    const last = merged[merged.length - 1];
    // Direkt angrenzend (nächster Tag nach dem bisherigen Ende) zählt auch
    // als zusammenhängend, damit z.B. "Ferien Land A bis 16., Land B ab 17."
    // nicht künstlich als zwei getrennte grüne Balken mit Lücke erscheint
    const nextDayAfterLast = new Date(last.endDate);
    nextDayAfterLast.setDate(nextDayAfterLast.getDate() + 1);
    const nextDayStr = nextDayAfterLast.toLocaleDateString('sv-SE');

    if (current.startDate <= nextDayStr) {
      if (current.endDate > last.endDate) last.endDate = current.endDate;
    } else {
      merged.push({ ...current });
    }
  }
  return merged;
}

async function fetchHolidaysForRegion(endpoint, region, validFrom, validTo) {
  try {
    let url = `https://openholidaysapi.org/${endpoint}?countryIsoCode=${region.country}&validFrom=${validFrom}&validTo=${validTo}&languageIsoCode=DE`;
    if (region.subdivision) url += `&subdivisionCode=${region.subdivision}`;

    const res = await fetchWithTimeout(url, {}, 8000);
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

  // Alle 16 Bundesländer einzeln laden, NUR für die "Deutschland gesamt"-
  // Vereinigung im neuen Ferienkalender-Tab - fließt NICHT in
  // is_school_holiday/holiday_countries (Attraktions-Prognosen) ein, das
  // bleibt bewusst bei NRW als repräsentativer Region für den Park.
  const germanStatesRaw = [];
  for (const state of GERMAN_STATE_CODES) {
    const region = { country: 'DE', subdivision: state.code, label: state.name };
    const school = await fetchHolidaysForRegion('SchoolHolidays', region, validFrom, validTo);
    germanStatesRaw.push(...school);
  }

  // Zeiträume der 16 Bundesländer zu einer einzigen "Deutschland"-Zeile
  // vereinigen (Union): sich überlappende/berührende Zeiträume werden zu
  // einem durchgehenden Zeitraum zusammengeführt, damit die Deutschland-Zeile
  // im Kalender nicht aus 16-fach übereinandergelegten Einzelbalken besteht,
  // sondern eine klare "irgendwo in Deutschland sind Ferien"-Linie zeigt -
  // exakt wie im gewünschten Referenzdesign (schulferien.org).
  const germanUnion = mergeDateRanges(germanStatesRaw.map(h => ({ startDate: h.startDate, endDate: h.endDate })));

  holidayCache = {
    schoolHolidays: allSchoolHolidays,
    publicHolidays: allPublicHolidays,
    germanUnion,
    lastFetched: Date.now()
  };

  console.log(`-> ${allSchoolHolidays.length} Schulferien-Zeiträume (NRW+Nachbarländer), ${allPublicHolidays.length} Feiertage, ${germanUnion.length} vereinigte Deutschland-Zeiträume (alle 16 Bundesländer) geladen.`);
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
      const res = await fetchWithTimeout(`https://queue-times.com/parks/${park.id}/queue_times.json`, {}, 10000);

      if (!res.ok) continue;

      const data = await res.json();
      let rides = [];
      if (data.rides) rides.push(...data.rides);
      if (data.lands) {
        data.lands.forEach(land => {
          if (land.rides) rides.push(...land.rides);
        });
      }

      // Mindestanzahl an Attraktionen, die queue-times.com für diesen Park
      // liefern muss, damit dieser Messpunkt als "vollständig" gilt und für
      // Besucherzahl-/Auslastungs-Berechnungen genutzt werden darf. Fehlen
      // kurzzeitig Attraktionen in der API-Antwort (kommt gelegentlich vor,
      // meist nur für einen Zyklus), würde das sonst einen künstlichen
      // Einbruch im Andrangs-Graphen erzeugen. Wartezeiten selbst werden
      // trotzdem ganz normal weiter gespeichert - nur das Auslastungs-Flag
      // wird auf 0 gesetzt, damit das Frontend diesen Zeitpunkt beim
      // Besucherzahl-Chart überspringen kann.
      const MIN_RIDES_FOR_CROWD_DATA = 32;
      const isCompleteSnapshot = rides.length >= MIN_RIDES_FOR_CROWD_DATA ? 1 : 0;

      const statements = rides.map(r => ({
        sql: `INSERT INTO wait_times
              (park_id, ride_id, ride_name, is_open, wait_time, recorded_at, recorded_date, recorded_time, weekday,
               temperature, precipitation, weather_code, is_school_holiday, holiday_countries, is_public_holiday, is_complete_snapshot)
              VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
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
          publicHolidayInfo.isHoliday ? 1 : 0,
          isCompleteSnapshot
        ]
      }));

      if (statements.length > 0) {
        await db.batch(statements, 'write');
      }

      if (!isCompleteSnapshot) {
        console.log(`⚠️ ${park.name} (${park.id}): nur ${rides.length} Attraktionen gefunden (< ${MIN_RIDES_FOR_CROWD_DATA}) - Wartezeiten gespeichert, aber als unvollständig für Auslastungs-Berechnung markiert.`);
      } else {
        console.log(`-> ${park.name} (${park.id}): ${rides.length} Attraktionen gespeichert.`);
      }

    } catch (err) {
      console.error(`Fehler beim Abruf für ${park.name}:`, err.message);
    }
  }

  lastFetchTimestamp = Date.now();
}

// Wartezeiten (und damit auch das aktuelle Wetter, siehe fetchAndSaveData)
// werden jetzt alle 5 Minuten abgerufen und gespeichert, statt alle 15
// Minuten. Das gilt für ALLE Attraktionen und für den Wetter-Snapshot, der
// bei jedem Speichervorgang mit aktualisiert wird - ein separater 5-Minuten-
// Wetter-Cronjob ist dadurch nicht mehr nötig, da beide jetzt denselben Takt
// haben und der Wetter-Cache ohnehin bei jedem fetchAndSaveData()-Durchlauf
// mit aktualisiert wird.
cron.schedule('*/5 * * * *', () => {
  fetchAndSaveData();
});

app.get('/health', (req, res) => {
  res.status(200).json({
    status: 'ok',
    timestamp: new Date().toISOString(),
    lastDataFetch: lastFetchTimestamp ? new Date(lastFetchTimestamp).toISOString() : null
  });
});

// ---------- ÖFFNUNGSZEITEN ABRUFEN (für heute, mit Fehler-Transparenz) ----------
// Liefert die aktuell bekannten Öffnungszeiten für heute PLUS Informationen
// darüber, wie zuverlässig diese sind: source ('scraped'/'fallback'),
// scrapeError (falls der letzte Scrape-Versuch fehlschlug, auch wenn dank
// Fallback trotzdem Zeiten verfügbar sind) und wie lange der letzte
// erfolgreiche Scrape her ist. Das Frontend nutzt scrapeError, um dem
// Nutzer sichtbar zu machen, dass der automatische Abruf gerade klemmt.
app.get('/api/opening-hours', async (req, res) => {
  const parkId = req.query.park || '56';
  const date = req.query.date || new Date().toLocaleDateString('sv-SE', { timeZone: 'Europe/Berlin' });

  try {
    const result = await db.execute({
      sql: `SELECT * FROM park_opening_hours WHERE park_id = ? AND date = ?`,
      args: [parkId, date]
    });

    const isToday = date === new Date().toLocaleDateString('sv-SE', { timeZone: 'Europe/Berlin' });

    if (result.rows.length === 0) {
      // Für heute sollte refreshOpeningHours() beim Start/stündlich bereits
      // einen Eintrag angelegt haben - falls nicht (z.B. ganz frischer Start,
      // Cron noch nicht gelaufen), hier synchron nachholen statt leer zu antworten
      if (isToday) {
        await refreshOpeningHours(parkId);
        const retry = await db.execute({
          sql: `SELECT * FROM park_opening_hours WHERE park_id = ? AND date = ?`,
          args: [parkId, date]
        });
        if (retry.rows.length > 0) {
          const row = retry.rows[0];
          return res.json({
            date, openTime: row.open_time, closeTime: row.close_time, source: row.source,
            closed: row.open_time === row.close_time,
            scrapeError: isToday ? lastOpeningHoursScrapeError : null,
            lastScrapeAttempt: lastOpeningHoursScrapeAttempt ? new Date(lastOpeningHoursScrapeAttempt).toISOString() : null
          });
        }
      }
      return res.json({ date, openTime: null, closeTime: null, source: 'none', closed: null, scrapeError: lastOpeningHoursScrapeError });
    }

    const row = result.rows[0];
    res.json({
      date,
      openTime: row.open_time,
      closeTime: row.close_time,
      source: row.source,
      closed: row.open_time === row.close_time,
      // scrapeError nur für HEUTE relevant zeigen - historische Tage sollen
      // nicht durch einen aktuellen Scrape-Fehler als "unzuverlässig" markiert wirken
      scrapeError: isToday ? lastOpeningHoursScrapeError : null,
      lastScrapeAttempt: lastOpeningHoursScrapeAttempt ? new Date(lastOpeningHoursScrapeAttempt).toISOString() : null
    });

  } catch (err) {
    console.error('Fehler in /api/opening-hours:', err.message);
    res.status(500).json({ error: 'Serverfehler.' });
  }
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

    // Öffnungszeiten werden bewusst NICHT hier dupliziert, sondern über den
    // dedizierten /api/opening-hours Endpoint geliefert (unterstützt auch
    // historische Daten und liefert den detaillierten Scrape-Fehlerstatus).
    // Das Frontend ruft beide Endpoints parallel ab.
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

// ---------- FERIENKALENDER-ÜBERSICHT (für den neuen "Ferien"-Tab) ----------
// Liefert eine schlanke, für den Tabellen-Kalender optimierte Struktur:
// pro "Zeile" (Deutschland gesamt + die 4 Nachbarländer) eine Liste von
// Ferien-Zeiträumen (bereits vereinigt bei Deutschland) sowie eine separate
// Liste aller Feiertage über alle Regionen. Bewusst getrennt von /api/context,
// da dort die Attraktions-Prognosen NRW-spezifische Daten brauchen, während
// der Kalender-Tab die vereinigte "irgendwo in Deutschland"-Sicht will.
app.get('/api/holidays-overview', async (req, res) => {
  try {
    if (holidayCache.lastFetched === 0) {
      await refreshHolidayCache();
    }

    // Feiertage aus allen Regionen sammeln, aber "Deutschland (NRW)" auf
    // "Deutschland" umbenennen, damit die Zeilenbeschriftung zur
    // zusammengefassten Ferien-Zeile passt (Feiertage wie der 3. Oktober
    // gelten ohnehin bundesweit, NRW-Feiertage sind repräsentativ genug)
    const publicHolidaysRenamed = holidayCache.publicHolidays.map(h => ({
      ...h,
      country: h.country === 'Deutschland (NRW)' ? 'Deutschland' : h.country
    }));

    res.json({
      rows: [
        { country: 'Deutschland', schoolHolidays: holidayCache.germanUnion },
        { country: 'Niederlande', schoolHolidays: holidayCache.schoolHolidays.filter(h => h.country === 'Niederlande') },
        { country: 'Belgien', schoolHolidays: holidayCache.schoolHolidays.filter(h => h.country === 'Belgien') },
        { country: 'Frankreich', schoolHolidays: holidayCache.schoolHolidays.filter(h => h.country === 'Frankreich') },
        { country: 'Luxemburg', schoolHolidays: holidayCache.schoolHolidays.filter(h => h.country === 'Luxemburg') }
      ],
      publicHolidays: publicHolidaysRenamed,
      lastRefreshed: holidayCache.lastFetched ? new Date(holidayCache.lastFetched).toISOString() : null
    });
  } catch (err) {
    console.error('Fehler in /api/holidays-overview:', err.message);
    res.status(500).json({ error: 'Serverfehler.' });
  }
});

app.get('/api/park', async (req, res) => {
  const parkId = req.query.park || '56';

  try {
    // Schwelle an den 5-Minuten-Speichertakt angepasst (statt vorher 10 Min
    // bei 15-Minuten-Takt) - etwas mehr als das doppelte Intervall, damit ein
    // einzelner verpasster Zyklus nicht sofort einen zusätzlichen Live-Abruf auslöst
    if (Date.now() - lastFetchTimestamp > 7 * 60 * 1000) {
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
          rides: [],
          isCompleteSnapshot: !!row.is_complete_snapshot
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

// ---------- AUSLASTUNGS-TAGE (für den neuen Auslastungs-Kalender) ----------
// Liefert alle Tage, an denen mindestens ein VOLLSTÄNDIGER Snapshot
// (is_complete_snapshot=1) existiert - nur diese Tage sind für den
// Auslastungs-Kalender im Statistik-Tab anklickbar/aussagekräftig.
app.get('/api/crowd-days', async (req, res) => {
  const parkId = req.query.park || '56';

  try {
    const result = await db.execute({
      sql: `SELECT DISTINCT recorded_date FROM wait_times WHERE park_id = ? AND is_complete_snapshot = 1 ORDER BY recorded_date ASC`,
      args: [parkId]
    });
    res.json({ days: result.rows.map(r => r.recorded_date) });
  } catch (err) {
    console.error('Fehler in /api/crowd-days:', err.message);
    res.status(500).json({ error: 'Serverfehler.' });
  }
});

// ---------- AUSLASTUNGS-VERLAUF (Einzeltag ODER über Zeitraum gemittelt) ----------
// Liefert für jeden Zeit-Slot eines Tages (oder gemittelt über mehrere Tage)
// ALLE Ride-Wartezeiten dieses Zeitpunkts, damit das Frontend daraus mit der
// bekannten Kapazitäts-Formel die geschätzte Besucherzahl berechnen kann -
// exakt wie im Live-Tab, nur eben historisch statt live. Es werden nur
// vollständige Snapshots (is_complete_snapshot=1) einbezogen, damit kurzzeitig
// fehlende Attraktionen die Auslastungsschätzung nicht künstlich einbrechen
// lassen (siehe MIN_RIDES_FOR_CROWD_DATA in fetchAndSaveData).
app.get('/api/crowd-history', async (req, res) => {
  const parkId = req.query.park || '56';
  const date = req.query.date;
  const days = Math.min(parseInt(req.query.days, 10) || 30, 90);

  try {
    if (date) {
      // Modus 1: Einzelner Tag - alle Ride-Wartezeiten pro Zeitpunkt
      const result = await db.execute({
        sql: `
          SELECT recorded_time, recorded_at, ride_name, is_open, wait_time
          FROM wait_times
          WHERE park_id = ? AND recorded_date = ? AND is_complete_snapshot = 1
          ORDER BY recorded_at ASC
        `,
        args: [parkId, date]
      });

      const grouped = {};
      result.rows.forEach(row => {
        if (!grouped[row.recorded_at]) {
          grouped[row.recorded_at] = { time: row.recorded_time, timestamp: row.recorded_at, rides: [] };
        }
        grouped[row.recorded_at].rides.push({ name: row.ride_name, isOpen: !!row.is_open, waitTime: row.wait_time });
      });

      res.json({ mode: 'single-day', date, history: Object.values(grouped).sort((a, b) => a.timestamp - b.timestamp) });

    } else {
      // Modus 2: Über Zeitraum gemittelt - pro Zeit-Slot der Ø über alle Tage,
      // getrennt nach Attraktion (damit die Kapazitäts-Formel weiterhin pro
      // Ride angewendet werden kann, statt einen Gesamt-Ø zu verfälschen)
      const cutoff = new Date();
      cutoff.setDate(cutoff.getDate() - days);
      const cutoffDate = cutoff.toLocaleDateString('sv-SE', { timeZone: 'Europe/Berlin' });

      const result = await db.execute({
        sql: `
          SELECT
            recorded_time,
            ride_name,
            AVG(CASE WHEN is_open = 1 THEN wait_time ELSE NULL END) as avg_wait,
            COUNT(DISTINCT recorded_date) as days_counted
          FROM wait_times
          WHERE park_id = ? AND recorded_date >= ? AND is_complete_snapshot = 1
          GROUP BY recorded_time, ride_name
          ORDER BY recorded_time ASC, ride_name ASC
        `,
        args: [parkId, cutoffDate]
      });

      const grouped = {};
      result.rows.forEach(row => {
        if (row.avg_wait === null) return;
        if (!grouped[row.recorded_time]) grouped[row.recorded_time] = { time: row.recorded_time, rides: [] };
        grouped[row.recorded_time].rides.push({
          name: row.ride_name,
          isOpen: true,
          waitTime: Math.round(row.avg_wait)
        });
      });

      res.json({ mode: 'averaged', days, history: Object.values(grouped).sort((a, b) => a.time.localeCompare(b.time)) });
    }

  } catch (err) {
    console.error('Fehler in /api/crowd-history:', err.message);
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

      // Heutige (bzw. zuletzt bekannte) Öffnungszeiten holen, um die
      // Empfehlung strikt auf den Zeitraum ZWISCHEN Parköffnung und
      // -schließung (mit demselben 30-Min-Puffer vor Schließung wie bei der
      // Ausfallerkennung) zu begrenzen. Ohne diesen Filter konnte die
      // "beste Zeit"-Empfehlung auf Slots kurz vor der offiziellen Öffnung
      // fallen, wo einzelne Attraktionen durch fehlerhafte Live-Meldungen
      // künstlich niedrige Wartezeiten hatten - das ergab keinen Sinn, da der
      // Park zu dem Zeitpunkt noch gar nicht offen war.
      const todayForHours = new Date().toLocaleDateString('sv-SE', { timeZone: 'Europe/Berlin' });
      let hoursRow = await db.execute({
        sql: `SELECT open_time, close_time FROM park_opening_hours WHERE park_id = ? AND date = ?`,
        args: [parkId, todayForHours]
      });
      let openTimeBound = '09:00';
      let closeTimeBound = '19:00';
      if (hoursRow.rows.length > 0) {
        openTimeBound = hoursRow.rows[0].open_time || openTimeBound;
        closeTimeBound = hoursRow.rows[0].close_time || closeTimeBound;
      }
      // 30-Minuten-Puffer vor Schließung abziehen (gleiche Logik wie
      // PRE_CLOSING_GRACE_MINUTES bei der Ausfallerkennung)
      const [ch, cm] = closeTimeBound.split(':').map(Number);
      const closeBoundMinutes = Math.max(0, ch * 60 + cm - 30);
      const recommendationCutoffTime = `${String(Math.floor(closeBoundMinutes / 60)).padStart(2, '0')}:${String(closeBoundMinutes % 60).padStart(2, '0')}`;

      let bestSlot = null;
      result.rows.forEach(row => {
        if (row.avg_wait === null || row.sample_count < 3) return;
        // Slot muss innerhalb der Öffnungszeit liegen (mit Schließ-Puffer)
        if (row.recorded_time < openTimeBound || row.recorded_time > recommendationCutoffTime) return;
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
// Erkennt Ausfälle innerhalb der Betriebszeit einer Attraktion an einem Tag.
// closeTime (optional, "HH:MM") ist die bekannte Park-Schließzeit dieses
// Tages (aus park_opening_hours) - wird genutzt, um einen "Ausfall", der
// genau bis zum offiziellen Parkschluss andauert, NICHT als echten Ausfall
// gegen die Attraktion zu werten, sondern als normales Tagesende. Ohne
// closeTime (z.B. für sehr alte Daten ohne gespeicherte Öffnungszeiten)
// fällt die Funktion auf ihr bisheriges Verhalten zurück (letzter Messpunkt
// des Tages = Ende der Betriebszeit).
// Wie viele Minuten vor der bekannten Park-Schließzeit ein "Ausfall" nicht
// mehr als solcher gezählt wird, sondern als normales, ggf. vorgezogenes
// Ende der Attraktion (viele Warteschlangen schließen schon 15-30 Min vor
// dem eigentlichen Parkschluss, das ist kein technischer Defekt).
const PRE_CLOSING_GRACE_MINUTES = 30;

// Fallback-Schließzeit für historische Tage, an denen park_opening_hours
// (noch) keinen Eintrag hat - z.B. weil das Öffnungszeiten-Scraping erst
// nachträglich eingeführt wurde. OHNE diesen Fallback würde
// detectOutagesForDay() den Puffer für solche Tage gar nicht anwenden können
// und die letzten Messungen des Tages fälschlich als "Ausfallbeginn" zählen
// (führte zum Bug: 19-20 Uhr erschien als "häufigste Ausfallzeit", obwohl der
// Park da einfach nur regulär schließt). 19:00 ist die typische
// Phantasialand-Schließzeit außerhalb der Hauptsaison.
const DEFAULT_CLOSE_TIME_FALLBACK = '19:00';

function detectOutagesForDay(pointsChronological, closeTime = null) {
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
      // Nur als echten Ausfall werten, wenn er NICHT innerhalb der letzten
      // PRE_CLOSING_GRACE_MINUTES vor Parkschluss begonnen hat - gilt für
      // JEDEN Ausfall im Tagesverlauf, nicht nur einen laufenden am Tagesende,
      // da manche Attraktionen schon deutlich vor offiziellem Parkschluss
      // ihre Warteschlange regulär schließen.
      const startsBeforeClosingGrace = !closeTime
        || minutesBetweenTimes(outageStart.recorded_time, closeTime) > PRE_CLOSING_GRACE_MINUTES;
      if (startsBeforeClosingGrace) {
        outages.push({
          startTime: outageStart.recorded_time,
          endTime: p.recorded_time,
          startedAt: outageStart.recorded_at,
          endedAt: p.recorded_at
        });
      }
      outageStart = null;
    }
  }
  // Falls der Tag mit einem laufenden "Ausfall" endet: dieselbe Regel gilt
  // auch hier (siehe oben) - nur zählen, wenn er deutlich vor Parkschluss begann.
  if (outageStart !== null) {
    const last = pointsChronological[pointsChronological.length - 1];
    const outageIsBeforeClose = !closeTime || minutesBetweenTimes(outageStart.recorded_time, closeTime) > PRE_CLOSING_GRACE_MINUTES;
    if (outageIsBeforeClose) {
      outages.push({
        startTime: outageStart.recorded_time,
        endTime: last.recorded_time,
        startedAt: outageStart.recorded_at,
        endedAt: last.recorded_at,
        ongoing: true
      });
    }
  }

  return { outages, firstOpenTime: pointsChronological[firstOpenIndex].recorded_time, lastTime: pointsChronological[pointsChronological.length - 1].recorded_time };
}

// Minuten zwischen zwei "HH:MM"-Zeitstempeln (b - a), für Vergleiche wie
// "liegt der Ausfallbeginn deutlich vor Parkschluss?"
function minutesBetweenTimes(timeA, timeB) {
  const [ha, ma] = timeA.split(':').map(Number);
  const [hb, mb] = timeB.split(':').map(Number);
  return (hb * 60 + mb) - (ha * 60 + ma);
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

    // Bekannte Park-Schließzeit für diesen Tag laden, damit ein "Ausfall" der
    // exakt bis Parkschluss dauert nicht fälschlich als Attraktions-Ausfall
    // gewertet wird (siehe detectOutagesForDay)
    const hoursResult = await db.execute({
      sql: `SELECT close_time FROM park_opening_hours WHERE park_id = ? AND date = ?`,
      args: [parkId, targetDate]
    });
    const closeTime = hoursResult.rows.length > 0 ? hoursResult.rows[0].close_time : null;

    const { outages, firstOpenTime } = detectOutagesForDay(result.rows, closeTime);

    res.json({ date: targetDate, outages, firstOpenTime, parkCloseTime: closeTime });

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

    // Bekannte Park-Schließzeiten für alle betroffenen Tage in einem Rutsch
    // laden (statt einzeln pro Tag), damit "Ausfall genau bis Parkschluss"
    // auch hier korrekt NICHT als Attraktions-Ausfall gezählt wird
    const closeTimesResult = await db.execute({
      sql: `SELECT date, close_time FROM park_opening_hours WHERE park_id = ? AND date >= ?`,
      args: [parkId, cutoffDate]
    });
    const closeTimeByDate = {};
    closeTimesResult.rows.forEach(r => { closeTimeByDate[r.date] = r.close_time; });

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

    Object.entries(byDay).forEach(([date, dayPoints]) => {
      const { outages, firstOpenTime, lastTime } = detectOutagesForDay(dayPoints, closeTimeByDate[date] || DEFAULT_CLOSE_TIME_FALLBACK);
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

// =====================================================================
// ================ INTELLIGENTE ÄHNLICHKEITS-PROGNOSE ================
// =====================================================================
// Statt einfach den Ø der letzten N Tage für denselben Wochentag+Uhrzeit-Slot
// zu nehmen, sucht dieser Ansatz unter ALLEN verfügbaren historischen Tagen
// diejenigen, die dem heutigen Tag am ähnlichsten sind - nach Wochentyp
// (Wochenende/Werktag), Schulferien-Situation (welche Länder betroffen),
// Feiertag, Wetterlage (Regen/trocken, Temperaturband) UND dem tatsächlichen
// bisherigen bzw. gesamten Besucheraufkommen des Tages. Nur diese ähnlichen
// Tage fließen dann gewichtet in die Prognose ein. Das ist bewusst rechen-
// aufwändig und wird deshalb serverseitig gecacht (siehe forecast_cache).

// Wandelt einen Wettercode in eine grobe Kategorie um (für den Ähnlichkeits-
// vergleich reicht "regnet es" + "wie warm", ein exakter Codevergleich wäre
// zu streng und würde kaum je zwei "identische" Tage finden)
function weatherCategoryFromCode(code) {
  if (code === null || code === undefined) return 'unbekannt';
  if (code === 0 || code === 1) return 'klar';
  if (code <= 3) return 'bewoelkt';
  if (code >= 45 && code <= 48) return 'nebel';
  if (code >= 51 && code <= 67) return 'regen';
  if (code >= 71 && code <= 77) return 'schnee';
  if (code >= 80 && code <= 82) return 'schauer';
  if (code >= 95) return 'gewitter';
  return 'sonstiges';
}
function temperatureBand(temp) {
  if (temp === null || temp === undefined) return 'unbekannt';
  if (temp < 5) return 'kalt';
  if (temp < 15) return 'kuehl';
  if (temp < 22) return 'mild';
  if (temp < 28) return 'warm';
  return 'heiss';
}

// Baut ein "Tagesprofil" aus den in wait_times gespeicherten Rohdaten für
// EINEN Tag (parkweit, über alle Attraktionen gemittelt) - wird sowohl für
// den heutigen Tag (bisheriger Verlauf) als auch für jeden historischen
// Vergleichstag gebraucht.
function buildDayProfile(dayRows, weekday) {
  if (dayRows.length === 0) return null;

  const isWeekend = weekday === 0 || weekday === 6;
  const isSchoolHoliday = dayRows.some(r => r.is_school_holiday === 1);
  const holidayCountriesSet = new Set();
  dayRows.forEach(r => { if (r.holiday_countries) r.holiday_countries.split(',').forEach(c => { if (c) holidayCountriesSet.add(c); }); });
  const isPublicHoliday = dayRows.some(r => r.is_public_holiday === 1);

  const weatherCodes = dayRows.map(r => r.weather_code).filter(c => c !== null && c !== undefined);
  const dominantWeatherCode = weatherCodes.length > 0
    ? weatherCodes.sort((a, b) =>
        weatherCodes.filter(v => v === a).length - weatherCodes.filter(v => v === b).length
      ).pop()
    : null;
  const temps = dayRows.map(r => r.temperature).filter(t => t !== null && t !== undefined);
  const avgTemp = temps.length > 0 ? temps.reduce((a, b) => a + b, 0) / temps.length : null;

  // Besucheraufkommen-Proxy: Ø Wartezeit über alle offenen Attraktionen an
  // diesem Tag (einfach, aber wirkungsvoll - korreliert stark mit echtem Andrang)
  const openWaits = dayRows.filter(r => r.is_open === 1).map(r => r.wait_time);
  const avgWait = openWaits.length > 0 ? openWaits.reduce((a, b) => a + b, 0) / openWaits.length : null;

  return {
    isWeekend,
    isSchoolHoliday,
    holidayCountries: holidayCountriesSet,
    isPublicHoliday,
    weatherCategory: weatherCategoryFromCode(dominantWeatherCode),
    temperatureBand: temperatureBand(avgTemp),
    avgWait // Proxy fürs Besucheraufkommen
  };
}

// Ähnlichkeits-Score zwischen zwei Tagesprofilen: 0 (völlig unähnlich) bis 1
// (praktisch identisch). Gewichtung ist bewusst so gewählt, dass das
// TATSÄCHLICHE Besucheraufkommen (avgWait) am stärksten zählt - das ist die
// ehrlichste, direkteste Kennzahl dafür, wie voll der Park an einem Tag war,
// stärker als jeder einzelne Einflussfaktor für sich.
function dayProfileSimilarity(a, b) {
  if (!a || !b) return 0;
  let score = 0;
  let maxScore = 0;

  // Wochenende/Werktag: harter Faktor, da sich das Verhalten stark unterscheidet
  maxScore += 2; if (a.isWeekend === b.isWeekend) score += 2;

  // Schulferien: ja/nein UND möglichst dieselben Länder betroffen
  maxScore += 2;
  if (a.isSchoolHoliday === b.isSchoolHoliday) {
    score += 1;
    if (a.isSchoolHoliday) {
      const overlap = [...a.holidayCountries].filter(c => b.holidayCountries.has(c)).length;
      const union = new Set([...a.holidayCountries, ...b.holidayCountries]).size;
      score += union > 0 ? overlap / union : 1;
    } else {
      score += 1;
    }
  }

  maxScore += 1; if (a.isPublicHoliday === b.isPublicHoliday) score += 1;

  maxScore += 1; if (a.weatherCategory === b.weatherCategory) score += 1;
  maxScore += 1; if (a.temperatureBand === b.temperatureBand) score += 1;

  // Besucheraufkommen (avgWait) - stärkstes Gewicht, da direkteste Kennzahl
  maxScore += 3;
  if (a.avgWait !== null && b.avgWait !== null) {
    const diff = Math.abs(a.avgWait - b.avgWait);
    const relCloseness = Math.max(0, 1 - diff / Math.max(a.avgWait, b.avgWait, 10));
    score += relCloseness * 3;
  }

  return maxScore > 0 ? score / maxScore : 0;
}

const SIMILARITY_THRESHOLD = 0.55; // Mindest-Ähnlichkeit, damit ein Tag überhaupt einfließt
const MAX_SIMILAR_DAYS = 60;       // Deckel nach oben, um den Server nicht zu überlasten

// Baut für ALLE verfügbaren Tage eines Rides (über alle wait_times-Zeilen,
// nicht nur der letzten N Tage) das Tagesprofil und vergleicht mit dem
// heutigen (bisherigen) Profil. Gibt eine nach Ähnlichkeit sortierte Liste
// zurück. days_lookback begrenzt optional, wie weit zurück gesucht wird
// (0/undefined = unbegrenzt, "so viele wie sie findet").
async function findSimilarDays(parkId, rideName, todayProfile, daysLookback) {
  let sql = `
    SELECT recorded_date, weekday, is_open, wait_time, weather_code, temperature,
           is_school_holiday, holiday_countries, is_public_holiday
    FROM wait_times
    WHERE park_id = ? AND ride_name = ?
  `;
  const args = [parkId, rideName];
  if (daysLookback) {
    const cutoff = new Date();
    cutoff.setDate(cutoff.getDate() - daysLookback);
    sql += ` AND recorded_date >= ?`;
    args.push(cutoff.toLocaleDateString('sv-SE', { timeZone: 'Europe/Berlin' }));
  }

  const result = await db.execute({ sql, args });
  if (result.rows.length === 0) return [];

  const byDay = {};
  result.rows.forEach(row => {
    if (!byDay[row.recorded_date]) byDay[row.recorded_date] = [];
    byDay[row.recorded_date].push(row);
  });

  const today = new Date().toLocaleDateString('sv-SE', { timeZone: 'Europe/Berlin' });

  const scored = [];
  Object.entries(byDay).forEach(([date, rows]) => {
    if (date === today) return; // heutiger Tag selbst zählt nicht als "Vergleichstag"
    const weekday = rows[0].weekday;
    const profile = buildDayProfile(rows, weekday);
    const similarity = dayProfileSimilarity(todayProfile, profile);
    if (similarity >= SIMILARITY_THRESHOLD) {
      scored.push({ date, weekday, similarity, rows });
    }
  });

  scored.sort((a, b) => b.similarity - a.similarity);
  return scored.slice(0, MAX_SIMILAR_DAYS);
}

// Erzeugt einen kurzen "Fingerabdruck" der aktuellen Einflussfaktoren, um zu
// erkennen, ob eine neue Berechnung überhaupt nötig ist (z.B. wenn sich das
// Wetter seit der letzten Cache-Berechnung geändert hat) oder der Cache
// weiterhin gültig ist, auch wenn er älter als der reine Zeitstempel-Check
// erlauben würde.
function buildFactorsFingerprint(todayProfile, currentHour) {
  return JSON.stringify({
    w: todayProfile.isWeekend,
    sh: todayProfile.isSchoolHoliday,
    hc: [...todayProfile.holidayCountries].sort(),
    ph: todayProfile.isPublicHoliday,
    wc: todayProfile.weatherCategory,
    tb: todayProfile.temperatureBand,
    // avgWait grob gebändert (nicht exakt), damit kleine Schwankungen keine
    // Neuberechnung erzwingen, ein wirklich anderer Andrang aber schon
    aw: todayProfile.avgWait !== null ? Math.round(todayProfile.avgWait / 5) * 5 : null,
    hour: currentHour
  });
}

const FORECAST_CACHE_MAX_AGE_MS = 15 * 60 * 1000; // 15 Minuten

// Kernfunktion: berechnet (oder liefert aus dem Cache) die intelligente
// Prognose für eine Attraktion. Liefert:
// - forecastSlots: Vorhersage für die nächsten paar Stunden (nicht den ganzen
//   Tag), berechnet aus dem gewichteten Mittel der ähnlichsten Tage
// - similarDaysCount: wie viele Tage tatsächlich einflossen
// - riskWindows: Zeitfenster (können mehrere sein, mit Range), in denen bei
//   den ähnlichen Tagen historisch am häufigsten Ausfälle auftraten
async function computeSmartForecast(parkId, rideName) {
  const now = new Date();
  const nowMinutes = now.getHours() * 60 + now.getMinutes();
  const currentHour = now.getHours();

  // Cache prüfen
  const cacheRow = await db.execute({
    sql: `SELECT computed_at, factors_fingerprint, result_json FROM forecast_cache WHERE park_id = ? AND ride_name = ?`,
    args: [parkId, rideName]
  });

  // Heutiges (bisheriges) Tagesprofil bauen - Basis für den Ähnlichkeitsvergleich
  const today = now.toLocaleDateString('sv-SE', { timeZone: 'Europe/Berlin' });
  const todayRowsResult = await db.execute({
    sql: `SELECT is_open, wait_time, weather_code, temperature, is_school_holiday, holiday_countries, is_public_holiday
          FROM wait_times WHERE park_id = ? AND recorded_date = ?`,
    args: [parkId, today]
  });
  const weekday = now.getDay();
  const todayProfile = buildDayProfile(todayRowsResult.rows, weekday) || {
    isWeekend: weekday === 0 || weekday === 6, isSchoolHoliday: false, holidayCountries: new Set(),
    isPublicHoliday: false, weatherCategory: 'unbekannt', temperatureBand: 'unbekannt', avgWait: null
  };

  const fingerprint = buildFactorsFingerprint(todayProfile, currentHour);

  if (cacheRow.rows.length > 0) {
    const cached = cacheRow.rows[0];
    const age = Date.now() - cached.computed_at;
    if (age < FORECAST_CACHE_MAX_AGE_MS && cached.factors_fingerprint === fingerprint) {
      return JSON.parse(cached.result_json);
    }
  }

  // Ähnliche Tage suchen (unbegrenzter Lookback - "so viele wie sie findet",
  // aber MAX_SIMILAR_DAYS als Deckel gegen Serverlast)
  const similarDays = await findSimilarDays(parkId, rideName, todayProfile, null);

  // Fallback: keine ausreichend ähnlichen Tage gefunden -> einfacher
  // Wochentags-Ø der letzten 60 Tage als Rückfallebene, klar markiert
  if (similarDays.length === 0) {
    const fallbackResult = await db.execute({
      sql: `
        SELECT recorded_time, AVG(CASE WHEN is_open=1 THEN wait_time ELSE NULL END) as avg_wait, COUNT(*) as n
        FROM wait_times
        WHERE park_id = ? AND ride_name = ? AND weekday = ? AND recorded_date >= ?
        GROUP BY recorded_time ORDER BY recorded_time ASC
      `,
      args: [parkId, rideName, weekday, new Date(Date.now() - 60 * 86400000).toLocaleDateString('sv-SE', { timeZone: 'Europe/Berlin' })]
    });
    const slots = fallbackResult.rows
      .filter(r => r.avg_wait !== null)
      .map(r => ({ time: r.recorded_time, forecast: Math.round(r.avg_wait), confidence: 'niedrig', basis: 'wochentag-fallback' }));

    const result = { slots: filterToNextHours(slots, nowMinutes), similarDaysCount: 0, method: 'fallback', riskWindows: [] };
    await saveForecastCache(parkId, rideName, fingerprint, result);
    return result;
  }

  // Gewichtete Prognose je Zeit-Slot aus den ähnlichen Tagen berechnen -
  // Gewicht = Ähnlichkeits-Score, damit sehr ähnliche Tage stärker zählen als
  // nur knapp über der Schwelle liegende
  const slotAccumulator = {}; // { "HH:MM": { weightedSum, weightTotal } }

  // recorded_time steht nicht in den oben aus findSimilarDays gelieferten
  // rows (wurde dort nicht selektiert) - daher hier gezielt nachladen, aber
  // NUR für die bereits gefundenen ähnlichen Tage (kein voller Tabellenscan)
  const similarDates = similarDays.map(d => d.date);
  const placeholders = similarDates.map(() => '?').join(',');
  const detailResult = await db.execute({
    sql: `
      SELECT recorded_date, recorded_time, is_open, wait_time
      FROM wait_times
      WHERE park_id = ? AND ride_name = ? AND recorded_date IN (${placeholders})
    `,
    args: [parkId, rideName, ...similarDates]
  });
  const similarityByDate = {};
  similarDays.forEach(d => { similarityByDate[d.date] = d.similarity; });

  detailResult.rows.forEach(row => {
    if (row.is_open !== 1) return; // Ausfälle fließen nicht in den Prognosewert ein (siehe Anforderung)
    const weight = similarityByDate[row.recorded_date] || 0;
    if (weight <= 0) return;
    if (!slotAccumulator[row.recorded_time]) slotAccumulator[row.recorded_time] = { weightedSum: 0, weightTotal: 0, n: 0 };
    slotAccumulator[row.recorded_time].weightedSum += row.wait_time * weight;
    slotAccumulator[row.recorded_time].weightTotal += weight;
    slotAccumulator[row.recorded_time].n++;
  });

  const allSlots = Object.entries(slotAccumulator)
    .filter(([, acc]) => acc.weightTotal > 0 && acc.n >= 2) // mind. 2 Datenpunkte pro Slot
    .map(([time, acc]) => ({
      time,
      forecast: Math.round(acc.weightedSum / acc.weightTotal),
      confidence: acc.n >= 8 ? 'hoch' : acc.n >= 4 ? 'mittel' : 'niedrig',
      basis: 'aehnliche-tage',
      sampleCount: acc.n
    }))
    .sort((a, b) => a.time.localeCompare(b.time));

  // Risiko-Zeitfenster: bei den ähnlichen Tagen die Uhrzeiten sammeln, zu
  // denen Ausfälle begannen, und zu zusammenhängenden Ranges gruppieren
  // (z.B. "12:00-13:00" statt einzelner Minutenwerte) - Parkschluss-nahe
  // "Ausfälle" wurden serverseitig in detectOutagesForDay() bereits
  // rausgefiltert (siehe PRE_CLOSING_GRACE_MINUTES), fließen hier also nicht
  // fälschlich als Risiko-Zeit ein.
  const riskWindows = await computeRiskWindows(parkId, rideName, similarDates);

  const result = {
    slots: filterToNextHours(allSlots, nowMinutes),
    similarDaysCount: similarDays.length,
    method: 'aehnlichkeit',
    avgSimilarity: Math.round((similarDays.reduce((s, d) => s + d.similarity, 0) / similarDays.length) * 100) / 100,
    riskWindows
  };

  await saveForecastCache(parkId, rideName, fingerprint, result);
  return result;
}

// Begrenzt die Prognose auf die nächsten paar Stunden ab jetzt (nicht den
// ganzen Tag) - wie gewünscht reicht ein realistischer Vorschau-Horizont,
// weiter in der Zukunft liegende Slots werden ohnehin unsicherer.
const FORECAST_HORIZON_MINUTES = 240; // 4 Stunden
function filterToNextHours(slots, nowMinutes) {
  return slots.filter(s => {
    const [h, m] = s.time.split(':').map(Number);
    const slotMinutes = h * 60 + m;
    const delta = slotMinutes - nowMinutes;
    return delta >= -15 && delta <= FORECAST_HORIZON_MINUTES; // kleiner Puffer rückwärts für nahtlosen Übergang
  });
}

async function saveForecastCache(parkId, rideName, fingerprint, result) {
  try {
    await db.execute({
      sql: `INSERT INTO forecast_cache (park_id, ride_name, computed_at, factors_fingerprint, result_json)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(park_id, ride_name) DO UPDATE SET computed_at=excluded.computed_at, factors_fingerprint=excluded.factors_fingerprint, result_json=excluded.result_json`,
      args: [parkId, rideName, Date.now(), fingerprint, JSON.stringify(result)]
    });
  } catch (err) {
    console.error('Fehler beim Cachen der Prognose:', err.message);
  }
}

// Ermittelt Zeitfenster mit gehäuften Ausfällen unter den ähnlichen Tagen.
// Gruppiert Ausfall-Startzeiten in Stunden-Buckets, findet Buckets mit
// überdurchschnittlich vielen Ausfällen, und fasst benachbarte auffällige
// Buckets zu einer Range zusammen (z.B. "12:00-13:00" statt zwei einzelnen
// Meldungen für 12 und 13 Uhr).
async function computeRiskWindows(parkId, rideName, similarDates) {
  if (similarDates.length === 0) return [];

  const placeholders = similarDates.map(() => '?').join(',');
  const rowsResult = await db.execute({
    sql: `
      SELECT recorded_date, recorded_time, recorded_at, is_open
      FROM wait_times
      WHERE park_id = ? AND ride_name = ? AND recorded_date IN (${placeholders})
      ORDER BY recorded_date ASC, recorded_at ASC
    `,
    args: [parkId, rideName, ...similarDates]
  });

  const byDay = {};
  rowsResult.rows.forEach(row => {
    if (!byDay[row.recorded_date]) byDay[row.recorded_date] = [];
    byDay[row.recorded_date].push(row);
  });

  // Für jeden Tag die bekannte Schließzeit holen (für den 30-Min-Puffer)
  const closeTimesResult = await db.execute({
    sql: `SELECT date, close_time FROM park_opening_hours WHERE park_id = ? AND date IN (${placeholders})`,
    args: [parkId, ...similarDates]
  });
  const closeTimeByDate = {};
  closeTimesResult.rows.forEach(r => { closeTimeByDate[r.date] = r.close_time; });

  const hourBuckets = {}; // { 0-23: count }
  let totalDaysWithData = 0;
  Object.entries(byDay).forEach(([date, rows]) => {
    const { outages } = detectOutagesForDay(rows, closeTimeByDate[date] || DEFAULT_CLOSE_TIME_FALLBACK);
    if (outages.length > 0) totalDaysWithData++;
    outages.forEach(o => {
      const hour = parseInt(o.startTime.split(':')[0], 10);
      hourBuckets[hour] = (hourBuckets[hour] || 0) + 1;
    });
  });

  const totalOutages = Object.values(hourBuckets).reduce((a, b) => a + b, 0);
  if (totalOutages === 0) return [];

  // Nur Stunden aufnehmen, die überdurchschnittlich oft betroffen sind
  // (mind. 2 Vorkommnisse UND mind. 15% aller Ausfälle dieser Stunde)
  const significantHours = Object.entries(hourBuckets)
    .filter(([, count]) => count >= 2 && count / totalOutages >= 0.15)
    .map(([hour]) => parseInt(hour, 10))
    .sort((a, b) => a - b);

  if (significantHours.length === 0) return [];

  // Benachbarte Stunden zu Ranges zusammenfassen
  const windows = [];
  let rangeStart = significantHours[0];
  let rangeEnd = significantHours[0];
  for (let i = 1; i < significantHours.length; i++) {
    if (significantHours[i] === rangeEnd + 1) {
      rangeEnd = significantHours[i];
    } else {
      windows.push({ startHour: rangeStart, endHour: rangeEnd + 1, occurrences: hourBuckets[rangeStart] });
      rangeStart = significantHours[i];
      rangeEnd = significantHours[i];
    }
  }
  windows.push({ startHour: rangeStart, endHour: rangeEnd + 1, occurrences: hourBuckets[rangeStart] });

  return windows.map(w => ({
    startTime: `${String(w.startHour).padStart(2, '0')}:00`,
    endTime: `${String(w.endHour).padStart(2, '0')}:00`,
    // Anteil der ähnlichen Tage, an denen in diesem Fenster ein Ausfall begann
    frequencyPercent: totalDaysWithData > 0 ? Math.round((w.occurrences / totalDaysWithData) * 100) : null
  }));
}

// ---------- ENDPOINT: Intelligente Prognose ----------
app.get('/api/smart-day-forecast', async (req, res) => {
  const parkId = req.query.park || '56';
  const rideName = req.query.ride;

  if (!rideName) {
    return res.status(400).json({ error: 'ride Parameter erforderlich.' });
  }

  try {
    const result = await computeSmartForecast(parkId, rideName);
    res.json(result);
  } catch (err) {
    console.error('Fehler in /api/smart-day-forecast:', err.message);
    res.status(500).json({ error: 'Serverfehler.' });
  }
});

async function start() {
  try {
    await initDatabase();

    // Einmalige Bereinigung: alte Wartezeiten-Daten von Kings Island (60) und
    // Cedar Point (64) löschen - diese Parks wurden aus der App entfernt
    // (sie liefen vorher unnötig im 5-Minuten-Speicherzyklus mit, obwohl
    // nirgends in der App genutzt). Betrifft nur historische Altdaten dieser
    // beiden Fremd-Parks, Phantasialand-Daten sind davon nicht betroffen.
    // Idempotent: löscht bei jedem Start erneut, falls doch mal wieder was
    // reinrutschen sollte, ist aber nach dem ersten Lauf ein No-Op.
    try {
      const cleanup = await db.execute(`DELETE FROM wait_times WHERE park_id IN ('60', '64')`);
      if (cleanup.rowsAffected > 0) {
        console.log(`🧹 ${cleanup.rowsAffected} alte Wartezeiten-Zeilen von entfernten Fremd-Parks gelöscht.`);
      }
      const cleanupHours = await db.execute(`DELETE FROM park_opening_hours WHERE park_id IN ('60', '64')`);
      if (cleanupHours.rowsAffected > 0) {
        console.log(`🧹 ${cleanupHours.rowsAffected} alte Öffnungszeiten-Zeilen von entfernten Fremd-Parks gelöscht.`);
      }
    } catch (err) {
      console.error('Konnte alte Fremd-Park-Daten nicht bereinigen:', err.message);
    }

    await fetchAndSaveData();
    await refreshOpeningHours(); // sofort beim Start Öffnungszeiten laden, nicht erst zur nächsten vollen Stunde warten

    app.listen(PORT, () => {
      console.log(`ParkPulse Server läuft auf Port ${PORT}`);
    });
  } catch (err) {
    console.error('❌ Fehler beim Start:', err.message);
    process.exit(1);
  }
}

start();
