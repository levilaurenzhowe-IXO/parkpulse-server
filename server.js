const express = require('express');
const cors = require('cors');
const cron = require('node-cron');
const fetch = require('node-fetch');

const app = express();
const PORT = process.env.PORT || 3000;

app.use(cors());
app.use(express.json());

const historyData = {};

async function fetchAndSaveData() {
  console.log(`[${new Date().toISOString()}] Starte 15-Minuten Abruf...`);

  // Alle Parks nutzen jetzt die zuverlässige Queue-Times API mit ihren IDs!
  // 60 = Kings Island, 64 = Cedar Point, 56 = Phantasialand
  const parksToFetch = [
    { id: '60', name: 'Kings Island' },
    { id: '64', name: 'Cedar Point' },
    { id: '56', name: 'Phantasialand' }
  ];

  for (const park of parksToFetch) {
    try {
      let rides = [];
      const res = await fetch(`https://queue-times.com/parks/${park.id}/queue_times.json`);
      
      if (res.ok) {
        const data = await res.json();
        
        // Attraktionen aus Hauptliste und Unterbereichen (Lands) zusammenführen
        if (data.rides) rides.push(...data.rides);
        if (data.lands) {
          data.lands.forEach(land => {
            if (land.rides) rides.push(...land.rides);
          });
        }

        rides = rides.map(r => ({
          name: r.name,
          isOpen: r.is_open,
          waitTime: r.wait_time || 0
        }));
      }

      if (!historyData[park.id]) historyData[park.id] = [];
      
      const timestamp = new Date().toLocaleTimeString('de-DE', { hour: '2-digit', minute: '2-digit' });
      historyData[park.id].push({
        time: timestamp,
        rides: rides
      });

      // Maximal 96 Messpunkte aufbewahren (24 Stunden)
      if (historyData[park.id].length > 96) {
        historyData[park.id].shift();
      }

      console.log(`-> ${park.name}: ${rides.length} Attraktionen gefunden.`);

    } catch (err) {
      console.error(`Fehler bei ${park.name}:`, err.message);
    }
  }
}

// Alle 15 Minuten ausführen
cron.schedule('*/15 * * * *', () => {
  fetchAndSaveData();
});

// Beim Start sofort ausführen
fetchAndSaveData();

// API Endpunkte
app.get('/api/live', (req, res) => {
  const parkId = req.query.park || '60';
  const parkHistory = historyData[parkId] || [];
  const latestEntry = parkHistory[parkHistory.length - 1];

  if (!latestEntry) {
    return res.status(404).json({ error: 'Noch keine Daten vorhanden' });
  }

  res.json(latestEntry);
});

app.get('/api/history', (req, res) => {
  const parkId = req.query.park || '60';
  res.json(historyData[parkId] || []);
});

app.listen(PORT, () => {
  console.log(`ParkPulse Server läuft auf Port ${PORT}`);
});
