import express from 'express';
import path from 'path';
import { fileURLToPath } from 'url';
import { EventEmitter } from 'events';

const app = express();
app.set('trust proxy', 1); // Heroku: hariyatama client IP eka (web login rate-limit ekata)
const __filename = fileURLToPath(import.meta.url);
const __path = path.dirname(__filename);
const PORT = process.env.PORT || 8000;

import { router as code } from './pair.js';

EventEmitter.defaultMaxListeners = 500;

// Middleware FIRST (මේ order එක වැදගත්)
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// Routes
app.use('/code', code);

app.get('/pair', (req, res) => {
  res.sendFile(path.join(__path, 'main.html'));
});

app.get('/', (req, res) => {
  res.sendFile(path.join(__path, 'main.html'));
});

app.listen(PORT, () => {
  console.log(`
 ██████╗  ██╗  ██╗ ██████╗ ███████╗████████╗
██╔════╝  ██║  ██║██╔═══██╗██╔════╝╚══██╔══╝
██║  ███╗ ███████║██║   ██║███████╗   ██║   
██║   ██║ ██╔══██║██║   ██║╚════██║   ██║   
╚██████╔╝ ██║  ██║╚██████╔╝███████║   ██║   
 ╚═════╝  ╚═╝  ╚═╝ ╚═════╝ ╚══════╝   ╚═╝   

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
💖 Don't Forget To Give a Star
🌐 URL    : http://localhost:${PORT}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
`);
});

export default app;
