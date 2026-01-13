const { Pool } = require('pg');
const path = require('path');
const loadConfig = require('./config/envLoader'); // or '../config/envLoader' depending on location

const configPath = path.resolve(__dirname, 'config/app.conf');
const config = loadConfig(configPath);

const pool = new Pool({
  user: config.DB_USER,
  host: config.DB_HOST,
  database: config.DB_NAME,
  password: config.DB_PASSWORD,
  port: config.DB_PORT,
});


module.exports = pool;
