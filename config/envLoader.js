const fs = require('fs');

function loadConfig(filePath) {
  const content = fs.readFileSync(filePath, 'utf-8');
  const lines = content.split(/\r?\n/);

  const config = {};
  let currentKey = null;
  let buffer = [];
  let bracketDepth = 0;

  for (let line of lines) {
    line = line.trim();

    if (!line || line.startsWith('#')) continue;

    // 🔁 If currently reading multiline JSON
    if (currentKey) {
      buffer.push(line);

      // Track brackets safely
      bracketDepth += (line.match(/\{/g) || []).length;
      bracketDepth += (line.match(/\[/g) || []).length;
      bracketDepth -= (line.match(/\}/g) || []).length;
      bracketDepth -= (line.match(/\]/g) || []).length;

      // End when all brackets are closed
      if (bracketDepth === 0) {
        config[currentKey] = buffer.join('\n');
        currentKey = null;
        buffer = [];
      }

      continue;
    }

    // 🧩 Normal key=value parsing
    const index = line.indexOf('=');
    if (index === -1) continue;

    const key = line.substring(0, index).trim();
    const value = line.substring(index + 1).trim();

    // 🔥 Detect JSON start
    if (value.startsWith('{') || value.startsWith('[')) {
      currentKey = key;
      buffer.push(value);

      // Initialize bracket depth
      bracketDepth += (value.match(/\{/g) || []).length;
      bracketDepth += (value.match(/\[/g) || []).length;
      bracketDepth -= (value.match(/\}/g) || []).length;
      bracketDepth -= (value.match(/\]/g) || []).length;

      // Handle one-line JSON
      if (bracketDepth === 0) {
        config[currentKey] = value;
        currentKey = null;
        buffer = [];
      }

    } else {
      // ✅ Normal single-line value
      config[key] = value;
    }
  }

  return config;
}

module.exports = loadConfig;