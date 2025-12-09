// auth.js
const jwt = require('jsonwebtoken');
const { v4: uuidv4 } = require('uuid');
const pool = require('./db');
const logger = require('./logger'); // <-- import logger


const JWT_SECRET = 'c2658234a1c669cbbc1a02890f6a4bf0a2abb796b6cadc8b0bcc849f37ea072893ebdfce8810e92dfa44dbe8f55f2082002ea9207ceb0aa9aae1481d019a3dcb';

// generate and store token for a client
async function generateTokenForClient(clientName) {
    const jti = uuidv4();
    const payload = { client: clientName, jti };
    const token = jwt.sign(payload, JWT_SECRET);

    const text = `
    INSERT INTO client_tokens (client_name, jti, token, issued_at, expires_at, revoked)
    VALUES ($1, $2, $3, NOW(),  NOW() + INTERVAL '1 year', FALSE)
    RETURNING *
    `;
    const values = [clientName, jti, token];

    logger.downstream.info(`Token created for ${clientName}`);

    const result = await pool.query(text, values);
    return { token, jti, dbRecord: result.rows[0] };
}

// revoke all tokens for a client (set revoked = true)
async function revokeByClientName(clientName) {
  const text = `
    UPDATE client_tokens
    SET revoked = TRUE, revoked_at = NOW()
    WHERE client_name = $1 AND revoked = FALSE
  `;
  await pool.query(text, [clientName]);
  logger.downstream.info(`Token revoked for ${clientName}`);

}

// middleware: verify token signature + check DB record for jti
async function verifyTokenMiddleware(req, res, next) {
  try {
    const authHeader = req.headers['authorization'];
    if (!authHeader){
      logger.downstream.error('No token provided');
      return res.status(401).json({ message: 'No token provided' });
    }

    const token = authHeader.split(' ')[1];
    if (!token) {
      logger.downstream.error('Malformed auth header');
      return res.status(401).json({ message: 'Malformed auth header' });}

    let decoded;
    try {
      decoded = jwt.verify(token, JWT_SECRET);
    } catch (err) {
      logger.downstream.error('Invalid or expired token');
      return res.status(401).json({ message: 'Invalid or expired token', error: err.message });
    }

    const jti = decoded.jti;
    const clientName = decoded.client;

    const q = `SELECT revoked, expires_at FROM client_tokens WHERE jti = $1 LIMIT 1`;
    const { rows } = await pool.query(q, [jti]);
    const record = rows[0];

    if (!record) {
      logger.downstream.error('Token does not exist');
      return res.status(401).json({ message: 'Token does not exist' });
    }
    if (record.revoked) {
      logger.downstream.error('Token revoked');
      return res.status(401).json({ message: 'Token revoked' });
    }
    if (new Date(record.expires_at) < new Date()) {
      logger.downstream.info('Token expired');
      return res.status(401).json({ message: 'Token expired' });
    }

    req.client = clientName;
    req.jti = jti;
    next();
  } catch (err) {
    logger.downstream.error('verifyTokenMiddleware error', err);
    res.status(500).json({ message: 'Internal error' });
  }
}

module.exports = {
  generateTokenForClient,
  revokeByClientName,
  verifyTokenMiddleware
};
