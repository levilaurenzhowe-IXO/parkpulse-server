const express = require('express');
const cors = require('cors');
const cron = require('node-cron');
const fetch = require('node-fetch');

const app = express();
const PORT = process.env.PORT || 3000;

app.use(cors());
app.use(express.json());

// Verlaufsspeicher für alle Parks
const historyData = {
  '60': [], // Kings Island
  '64': [], // Cedar Point
  '56': []  // Phantasialand
};

let lastFetchTimestamp = 0;

async function fetchAndSaveData() {
  console.log(`[${new Date().toISOString()}] Starte Datenabruf für alle Parks...`);

  const parksToFetch = [
    { id: '60', name: 'Kings Island' },
    { id: '64', name: 'Cedar Point' },
    { id: '56', name: 'Phantasialand' }
  ];

  for (const park of parksToFetch) {
    try {
      const res = await fetch(`https://queue-times.com/parks/${park.id}/queue_times.json`);
      
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

        if (!historyData[park.id]) historyData[park.id] = [];

        historyData[park.id].push({
          time: timestamp,
          timestamp: Date.now(),
          rides: formattedRides
        });

        // Maximal 96 Messpunkte behalten (24 Stunden)
        if (historyData[park.id].length > 96) {
          historyData[park.id].shift();
        }

        console.log(`-> ${park.name} (${park.id}): ${formattedRides.length} Attraktionen gespeichert.`);
      }
    } catch (err) {
      console.error(`Fehler beim Abruf für ${park.name}:`, err.message);
    }
  }

  lastFetchTimestamp = Date.now();
}

// Timer: Alle 15 Minuten im Hintergrund ausführen
cron.schedule('*/15 * * * *', () => {
  fetchAndSaveData();
});

// Beim Start sofort abfragen
fetchAndSaveData();

// --- API ENDPUNKTE ---

// Verlaufs- und Live-Daten abrufen
app.get('/api/park', async (req, res) => {
  const parkId = req.query.park || '56';

  // Wenn der Server geschlafen hat (> 10 Min), sofort frische Daten holen
  if (Date.now() - lastFetchTimestamp > 10 * 60 * 1000) {
    console.log("Server war inaktiv, hole frische Daten...");
    await fetchAndSaveData();
  }

  const parkHistory = historyData[parkId] || [];

  res.json({
    history: parkHistory
  });
});

app.listen(PORT, () => {
  console.log(`ParkPulse Server läuft auf Port ${PORT}`);
});
