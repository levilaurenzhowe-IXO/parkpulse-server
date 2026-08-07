const express = require('express');
const cors = require('cors');
const cron = require('node-cron');
const fetch = require('node-fetch');

const app = express();
const PORT = process.env.PORT || 3000;

app.use(cors());
app.use(express.json());

const historyData = {};
let lastFetchTimestamp = 0; // Merkt sich, wann zuletzt Daten geholt wurden

async function fetchAndSaveData() {
  console.log(`[${new Date().toISOString()}] Starte Datenabruf...`);

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

      if (historyData[park.id].length > 96) {
        historyData[park.id].shift();
      }

      console.log(`-> ${park.name}: ${rides.length} Attraktionen gespeichert.`);

    } catch (err) {
      console.error(`Fehler bei ${park.name}:`, err.message);
    }
  }

  // Speicher-Zeitpunkt aktualisieren
  lastFetchTimestamp = Date.now();
}

// Timer für dauerhafte Hintergrund-Messungen (wenn Server wach ist)
cron.schedule('*/15 * * * *', () => {
  fetchAndSaveData();
});

// Beim Serverstart sofort ausführen
fetchAndSaveData();

// --- API Endpunkte ---

app.get('/api/live', async (req, res) => {
  const parkId = req.query.park || '60';

  // PRÜFUNG: Sind die Daten älter als 10 Minuten? (z.B. weil der Server geschlafen hat)
  const tenMinutesInMs = 10 * 60 * 1000;
  if (Date.now() - lastFetchTimestamp > tenMinutesInMs) {
    console.log("Daten veraltet (Server war inaktiv). Hole jetzt sofort neue Live-Daten...");
    await fetchAndSaveData(); // Sofort neue Daten holen!
  }

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
