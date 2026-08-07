const express = require('express');
const cors = require('cors');
const cron = require('node-cron');
const fetch = require('node-fetch');

const app = express();
const PORT = process.env.PORT || 3000;

app.use(cors());
app.use(express.json());

// Speicher für Verlaufsdaten (hält Daten der letzten 24h)
const historyData = [];
let lastFetchTimestamp = 0;

async function fetchAndSaveData() {
  console.log(`[${new Date().toISOString()}] Abruf Phantasialand (ID 56)...`);

  try {
    const res = await fetch(`https://queue-times.com/parks/56/queue_times.json`);
    
    if (res.ok) {
      const data = await res.json();
      let rides = [];

      if (data.rides) rides.push(...data.rides);
      if (data.lands) {
        data.lands.forEach(land => {
          if (land.rides) rides.push(...land.rides);
        });
      }

      const formattedRides = rides.map(r => ({
        id: r.id,
        name: r.name,
        isOpen: r.is_open,
        waitTime: r.wait_time || 0
      }));

      const timestamp = new Date().toLocaleTimeString('de-DE', { hour: '2-digit', minute: '2-digit' });

      // In der Historie speichern
      historyData.push({
        time: timestamp,
        timestamp: Date.now(),
        rides: formattedRides
      });

      // Maximal 96 Messpunkte behalten (96 * 15 Min = 24 Stunden)
      if (historyData.length > 96) {
        historyData.shift();
      }

      console.log(`-> Phantasialand: ${formattedRides.length} Attraktionen gespeichert.`);
    }
  } catch (err) {
    console.error(`Fehler beim Abruf:`, err.message);
  }

  lastFetchTimestamp = Date.now();
}

// Alle 15 Minuten automatisch abfragen
cron.schedule('*/15 * * * *', () => {
  fetchAndSaveData();
});

// Beim Start sofort abfragen
fetchAndSaveData();

// --- API ENDPUNKTE ---

// 1. Alle Verlaufsdaten des Tages abrufen
app.get('/api/phantasialand', async (req, res) => {
  // Wenn der Server geschlafen hat (> 10 Min), sofort frische Daten holen
  if (Date.now() - lastFetchTimestamp > 10 * 60 * 1000) {
    await fetchAndSaveData();
  }

  res.json({
    history: historyData
  });
});

app.listen(PORT, () => {
  console.log(`Phantasialand Pulse Server läuft auf Port ${PORT}`);
});
