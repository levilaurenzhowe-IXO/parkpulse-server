const express = require('express');
const cors = require('cors');
const cron = require('node-cron');
const fetch = require('node-fetch');

const app = express();
const PORT = process.env.PORT || 3000;

app.use(cors()); // CORS für alle Besucher erlauben
app.use(express.json());

// In-Memory Speicher für Verlaufsdaten (hält Daten der letzten 24h)
const historyData = {};

// -------------------------------------------------------------------
// Hauptfunktion: Daten von Parks abfragen & speichern
// -------------------------------------------------------------------
async function fetchAndSaveData() {
  console.log(`[${new Date().toISOString()}] Starte automatischen 15-Minuten Abruf...`);

  const parksToFetch = [
    { id: '60', type: 'US', name: 'Kings Island' },
    { id: '64', type: 'US', name: 'Cedar Point' },
    { id: 'phantasialand', type: 'EU', name: 'Phantasialand' }
  ];

  for (const park of parksToFetch) {
    try {
      let rides = [];

      if (park.type === 'US') {
        const res = await fetch(`https://queue-times.com/parks/${park.id}/queue_times.json`);
        if (res.ok) {
          const data = await res.json();
          if (data.rides) rides.push(...data.rides);
          if (data.lands) data.lands.forEach(l => l.rides && rides.push(...l.rides));
          rides = rides.map(r => ({ name: r.name, isOpen: r.is_open, waitTime: r.wait_time || 0 }));
        }
      } else {
        const res = await fetch(`https://api.wartezeiten.app/v1/waitingtimes?park=${park.id}&language=de`);
        if (res.ok) {
          const data = await res.json();
          if (Array.isArray(data)) {
            rides = data.map(r => ({ name: r.name, isOpen: r.status === 'opened', waitTime: r.waitingtime || 0 }));
          }
        }
      }

      // Daten im Verlauf ablegen
      if (!historyData[park.id]) historyData[park.id] = [];
      
      const timestamp = new Date().toLocaleTimeString('de-DE', { hour: '2-digit', minute: '2-digit' });
      historyData[park.id].push({
        time: timestamp,
        rides: rides
      });

      // Maximal 96 Einträge aufbewahren (96 * 15 Min = 24 Stunden)
      if (historyData[park.id].length > 96) {
        historyData[park.id].shift();
      }

    } catch (err) {
      console.error(`Fehler beim Abruf für ${park.name}:`, err.message);
    }
  }
}

// -------------------------------------------------------------------
// Cronjob: Alle 15 Minuten automatisch ausführen
// -------------------------------------------------------------------
cron.schedule('*/15 * * * *', () => {
  fetchAndSaveData();
});

// Beim Start des Servers direkt einmal Daten holen
fetchAndSaveData();

// -------------------------------------------------------------------
// API Endpunkte für deine HTML-Webseite
// -------------------------------------------------------------------

// 1. Live-Daten abrufen
app.get('/api/live', (req, res) => {
  const parkId = req.query.park || '60';
  const parkHistory = historyData[parkId] || [];
  const latestEntry = parkHistory[parkHistory.length - 1];

  if (!latestEntry) {
    return res.status(404).json({ error: 'Noch keine Daten vorhanden' });
  }

  res.json(latestEntry);
});

// 2. Verlaufs-Daten für Diagramme abrufen
app.get('/api/history', (req, res) => {
  const parkId = req.query.park || '60';
  res.json(historyData[parkId] || []);
});

// Server starten
app.listen(PORT, () => {
  console.log(`ParkPulse Server läuft auf Port ${PORT}`);
});