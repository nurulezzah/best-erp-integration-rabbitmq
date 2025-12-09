const fs = require('fs');
const path = require('path');
const { createLogger, format, transports } = require('winston');
require('winston-daily-rotate-file');

// Base log directory
const baseLogDir = '/var/log/best-erp-integration';

// Define subdirectories
const logDirs = {
  upstream: path.join(baseLogDir, 'upstream'),
  downstream: path.join(baseLogDir, 'downstream'),
};

// Ensure all directories exist
for (const dir of Object.values(logDirs)) {
  if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir, { recursive: true });
  }
}

// Helper: create logger that always writes to both info and error logs
function createLoggerWithErrorFile(name, dirPath) {
  const infoRotate = new transports.DailyRotateFile({
    dirname: dirPath,
    filename: `${name}-info-%DATE%.log`,
    datePattern: 'YYYY-MM-DD',
    level: 'info',
  });

  const errorRotate = new transports.DailyRotateFile({
    dirname: dirPath,
    filename: `${name}-error-%DATE%.log`,
    datePattern: 'YYYY-MM-DD',
    level: 'error',
  });

  return createLogger({
    level: 'info',
    format: format.combine(
      format.timestamp({ format: 'YYYY-MM-DD HH:mm:ss' }),
      format.printf(info => `[${info.timestamp}] ${info.level.toUpperCase()}: ${info.message}`)
    ),
    transports: [infoRotate, errorRotate],
  });
}

// Create loggers for both systems
const logger = {
  upstream: createLoggerWithErrorFile('upstream', logDirs.upstream),
  downstream: createLoggerWithErrorFile('downstream', logDirs.downstream),
};

module.exports = logger;
