const fs = require('fs');

function loadConfig(filePath) {
  const config = {};
  const data = fs.readFileSync(filePath, 'utf-8');

  data.split(/\r?\n/).forEach(line => {
    line = line.trim();
    if (!line || line.startsWith('#')) return; // skip empty lines or comments
    const index = line.indexOf('=');
    if (index === -1) return; // skip invalid lines
    const key = line.substring(0, index).trim();
    const value = line.substring(index + 1).trim();
    config[key] = value;
  });

  return config;
}

module.exports = loadConfig;
