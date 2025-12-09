const express = require('express');
const https = require('https');
const fs = require('fs');
const path = require('path');
const session = require('express-session');
const { generateTokenForClient, revokeByClientName } = require('./auth');
const broku = require('./broku-downstream/broku');
const logger = require('./logger');

const app = express();
const PORT = 2010;
const HOST = '0.0.0.0';

// =========================
// SSL Certificate Setup
// =========================
const sslOptions = {
  key: fs.readFileSync(path.join(__dirname, 'certs', 'erp-middleware.key')),
  cert: fs.readFileSync(path.join(__dirname, 'certs', 'erp-middleware.crt')),
};

// =========================
// Middleware
// =========================
app.use(express.json());

app.use(session({
  secret: '32da1215356e1ffc4eaa32f3d3daace837c4f763eb0972d1bca6e14f6cacc96ef9ca35f5f4eedebabd72d725375d5aa43d8243c2a96588768e655d37dd302cdc',
  resave: false,
  saveUninitialized: false,
  cookie: {
    maxAge: 1000 * 60 * 15,
    secure: true,
    httpOnly: true
  }
}));

// =========================
// Routes
// =========================

// Generate token
app.get('/generate-token/:client', async (req, res) => {
  try {
    const { client } = req.params;
    const result = await generateTokenForClient(client);
    res.json({
      message: 'Token generated successfully',
      client,
      token: result.token,
      jti: result.jti,
      expires_at: result.dbRecord.expires_at
    });
  } catch (err) {
    logger.downstream.error(`Error generating token: ${err.message}`);
    res.status(500).json({ message: 'Error generating token', error: err.message });
  }
});

// Revoke token
app.post('/revoke-token/:client', async (req, res) => {
  try {
    const { client } = req.params;
    await revokeByClientName(client);
    res.json({ message: `All tokens for client "${client}" have been revoked.` });
  } catch (err) {
    logger.downstream.error(`Error revoking token: ${err.message}`);
    res.status(500).json({ message: 'Error revoking token', error: err.message });
  }
});

// broku routes
app.use('/', broku);

// =========================
// HTTPS Server Setup
// =========================
const server = https.createServer(sslOptions, app);

//  Log successful SSL handshakes
server.on('secureConnection', (tlsSocket) => {
  const cert = tlsSocket.getPeerCertificate();
  const clientIP = tlsSocket.remoteAddress || 'Unknown IP';
  logger.downstream.info(`SSL handshake established with ${clientIP}`);

  if (cert && Object.keys(cert).length > 0) {
    logger.downstream.info(
      `Client certificate info: CN=${cert.subject?.CN || 'N/A'}, Issuer=${cert.issuer?.CN || 'N/A'}`
    );
  }
});

//  Log failed SSL handshakes
server.on('sslClientError', (err, socket) => {
  const clientIP = socket.remoteAddress || 'Unknown IP';
  logger.downstream.error(`SSL client error from ${clientIP}: ${err.message}`);
  socket.destroy(); // close the bad connection safely
});

// ⚠️ General HTTPS server errors (non-handshake)
server.on('error', (err) => {
  logger.downstream.error(`Server error: ${err.message}`, { stack: err.stack });
});

// Start HTTPS server
server.listen(PORT, HOST, () => {
  const message = `BEST Downstream HTTPS Server running at https://${HOST}:${PORT}`;
  console.log(message);
  logger.downstream.info(message);
});
