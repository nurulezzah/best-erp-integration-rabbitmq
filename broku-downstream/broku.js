// routes/downstreamRoutes.js
const express = require('express');
const router = express.Router();
const { processReq } = require('./broku-so');
const { getInventory } = require('./broku-inventory');
const { checkOrderStatus } = require('./broku-co');
const logger = require('../logger');
const { verifyTokenMiddleware } = require('../auth'); // protect routes
const { v4: uuidv4 } = require('uuid');




function sessionHandler(routeName) {
  return (req, res, next) => {
    const sessionId = uuidv4();
    req.session.requestId = sessionId;
    logger.downstream.info(`Session created for ${routeName}: ${sessionId}`);

    res.on('finish', () => {
      req.session.destroy(err => {
        if (err) {
          logger.downstream.error(`Error destroying session ${sessionId}:`, err);
        } else {
          logger.downstream.info(`Session destroyed for ${routeName}: ${sessionId}`);
        }
      });
    });

    next();
  };
}


router.post('/createOrder', verifyTokenMiddleware, sessionHandler('broku-so'), async (req, res) => {
  try {
    const inputData = req.body;
    logger.downstream.info(`Receive request: ${JSON.stringify(inputData)}`);

    const result = await processReq(inputData);
    logger.downstream.info(`Response: ${JSON.stringify(result)}`);

    res.json(result);
  } catch (err) {
    logger.downstream.error('Error processing createOrder:', err);
    res.status(500).json({ status: 'error', message: err.message });
  }
});

router.post('/getInventory', verifyTokenMiddleware, sessionHandler('broku-inventory'), async (req, res) => {
  try {
    const inputData = req.body;
    logger.downstream.info(`Receive request: ${JSON.stringify(inputData)}`);

    const result = await getInventory(inputData);
    logger.downstream.info(`Response: ${JSON.stringify(result)}`);

    res.json(result);
  } catch (err) {
    logger.downstream.error('Error processing getInventory:', err);
    res.status(500).json({ status: 'error', message: err.message });
  }
});

router.post('/checkStatus', verifyTokenMiddleware, sessionHandler('broku-co'), async (req, res) => {
  try {
    const inputData = req.body;
    logger.downstream.info(`Receive request: ${JSON.stringify(inputData)}`);

    const result = await checkOrderStatus(inputData);
    logger.downstream.info(`Response: ${JSON.stringify(result)}`);

    res.json(result);
  } catch (err) {
    logger.downstream.error('Error processing checkOrderStatus:', err);
    res.status(500).json({ status: 'error', message: err.message });
  }
});

module.exports = router;
