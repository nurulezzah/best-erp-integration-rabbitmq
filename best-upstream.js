const express = require('express');
const session = require('express-session');
const { v4: uuidv4 } = require('uuid'); // for generating unique IDs
const { processSalesOrder } = require('./upstream/salesorder');
const { checkOrderStatus } = require('./upstream/checkinventory');
const { checkStatus } = require('./upstream/checkorder');
const logger = require('./logger'); // <-- import logger


const app = express();
const port = 3000;
const hostname = "127.0.0.1";

// Import your salesorder function

// Middleware to parse JSON body
app.use(express.json());

// Session middleware setup
app.use(session({
  secret: 'd1117059e53ebd707f6b12c326264e0cb014638560ebd43d54f505fc5fe900a91a90ab50ae4a683c5437c6ea70ab61fe64d80402cf940cf7cfbf0232cd5263dd',   
  resave: false,
  saveUninitialized: false,
  cookie: {
    maxAge: 1000 * 60 * 15, // 15 minutes
    secure: false,           // set true if using HTTPS
    httpOnly: true
  }
}));



// CREATE A LOG WHEN THE APPLICATION JUST STARTED TO RUN
app.listen(port, hostname, () => {
  const message = `BEST Upstream server started and running at http://${hostname}:${port}`;
  logger.upstream.info(message);           // write to your log file
});




function sessionHandler(routeName) {
  return (req, res, next) => {
    const sessionId = uuidv4();
    req.session.requestId = sessionId;
    logger.upstream.info(`Session created for ${routeName}: ${sessionId}`);

    res.on('finish', () => {
      req.session.destroy(err => {
        if (err) {
          logger.upstream.error(`Error destroying session ${sessionId}:`, err);
        } else {
          logger.upstream.info(`Session destroyed for ${routeName}: ${sessionId}`);
        }
      });
    });

    next();
  };
}

// Apply middleware for both routes
app.use('/salesorder', sessionHandler('salesorder'));
app.use('/checkinventory', sessionHandler('checkinventory'));
app.use('/checkorder', sessionHandler('checkorder'));


// Route
app.post('/salesorder',  async (req, res) => {
  try {
    const inputData = req.body;

    logger.upstream.info(`Receive request from downstream: ${JSON.stringify(inputData, null, 2)}`);


    const result = await processSalesOrder(inputData);
    logger.upstream.info(`Return response to downstream: ${JSON.stringify(result)}`);

    res.json(result);
  } catch (err) {
    logger.upstream.error('Error processing sales order:', err);
    res.status(500).json({ status: 'error', message: err.message });
  }
});



app.post('/checkinventory',  async (req, res) => {
  try {
    const inputData = req.body;

    logger.upstream.info(`Receive request from downstream: ${JSON.stringify(inputData, null, 2)}`);


    const result = await checkOrderStatus(inputData);
    logger.upstream.info(`Return response to downstream: ${JSON.stringify(result)}`);

    res.json(result);
  } catch (err) {
    logger.upstream.error('Error processing Inventory checking:', err);
    res.status(500).json({ status: 'error', message: err.message });
  }
});

app.post('/checkorder',  async (req, res) => {
  try {
    const inputData = req.body;

    logger.upstream.info(`Receive request from downstream: ${JSON.stringify(inputData, null, 2)}`);


    const result = await checkStatus(inputData);
    logger.upstream.info(`Return response to downstream: ${JSON.stringify(result)}`);

    res.json(result);
  } catch (err) {
    logger.upstream.error('Error processing at check status :', err);
    res.status(500).json({ status: 'error', message: err.message });
  }
});


// Optional: test GET
app.get('/salesorder', (req, res) => {
    res.json({ message: 'SalesOrder route is working!' });
});

app.get('/checkinventory', (req, res) => {
    res.json({ message: 'Check Inventory route is working!' });
});

app.get('/checkorder', (req, res) => {
    res.json({ message: 'Check orderS route is working!' });
});

app.listen(port, () => {
    console.log(`Server running at http://${hostname}:${port}`);
});


