const amqp = require('amqplib');
const path = require('path');
const loadConfig = require('./config/envLoader');
const axios = require('axios');
const logger = require('./logger');

const configPath = path.resolve(__dirname, './config/app.conf');
const config = loadConfig(configPath);

const RABBIT_URL = config.RABBITMQ_URL;
// const RABBIT_URL = `amqp://localhost:5672`;

const QUEUE_HANDLERS = {
  sales_order: {
    endpoint: config.UPSTREAM_URL + config.ERP_SO_ENDPOINT
  },
  check_inventory: {
    endpoint: config.UPSTREAM_URL + config.ERP_INVENTORY_ENDPOINT
  },
  check_order: {
    endpoint: config.UPSTREAM_URL + config.ERP_CO_ENDPOINT
  }
};

let retryDelay = 2000;
const MAX_DELAY = 30000;

async function consumeQueue(channel, queueName, handler) {
  await channel.assertQueue(queueName, { durable: true });

  channel.consume(queueName, async (msg) => {
    if (!msg) return;

    let payload;
    try {
      payload = JSON.parse(msg.content.toString());
    } catch (e) {
      channel.ack(msg);
      return;
    }

    const { correlationId, replyTo } = msg.properties;

    let response;
    try {
      const erpResponse = await axios.post(handler.endpoint, payload);
      response = { success: true, data: erpResponse.data };
    } catch (err) {
      response = {
        success: false,
        error: err.response?.data || err.message
      };
    }

    channel.sendToQueue(
      replyTo,
      Buffer.from(JSON.stringify(response)),
      { correlationId }
    );

    channel.ack(msg);
  });

  logger.consumer.info(`Listening on ${queueName}`);
}

async function startConsumer() {
  while (true) {
    try {
      logger.consumer.info(`Connecting to RabbitMQ at ${RABBIT_URL}`);
      console.log(`Connecting to RabbitMQ`);
      
      const connection = await amqp.connect(RABBIT_URL + '?heartbeat=30');

      console.log(`Successfully connected to RabbitMQ`);

      connection.on('error', err => {
        logger.consumer.error('RabbitMQ connection error:', err.message);
      });

      connection.on('close', () => {
        logger.consumer.warn('RabbitMQ connection closed. Reconnecting...');
        console.log('RabbitMQ connection closed. Reconnecting...');

      });

      const channel = await connection.createChannel();
      channel.prefetch(1);

      for (const [queueName, handler] of Object.entries(QUEUE_HANDLERS)) {
        await consumeQueue(channel, queueName, handler);
      }

      logger.consumer.info('RabbitMQ consumer connected.');

      retryDelay = 2000; // reset after success

      // Wait until connection closes
      await new Promise(resolve => connection.once('close', resolve));

    } catch (err) {
      logger.consumer.error(`RabbitMQ connection failed. Retrying in ${retryDelay / 1000}s`, err.message);
      console.log(`RabbitMQ connection failed. Retrying in ${retryDelay / 1000}s`, err.message);

      await sleep(retryDelay);
      retryDelay = Math.min(retryDelay * 2, MAX_DELAY);
    }
  }
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// Start consumer
startConsumer().catch(err => {
  logger.consumer.error('Fatal consumer error:', err);
  console.log('Fatal consumer error:', err);

});
