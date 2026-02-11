const amqp = require('amqplib');
const path = require('path');
const loadConfig = require('./config/envLoader');
const axios = require('axios');
const logger = require('./logger');

const configPath = path.resolve(__dirname, './config/app.conf');
const config = loadConfig(configPath);

const RABBIT_URL = config.RABBITMQ_URL;

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

// ------------------ Token Bucket ------------------
class TokenBucket {
  constructor({ capacity, refillRate }) {
    this.capacity = capacity;
    this.tokens = capacity;
    this.refillRate = refillRate; // tokens per second
    this.lastRefill = Date.now();
  }

  refill() {
    const now = Date.now();
    const elapsedSeconds = (now - this.lastRefill) / 1000;
    const tokensToAdd = elapsedSeconds * this.refillRate;

    if (tokensToAdd > 0) {
      this.tokens = Math.min(this.capacity, this.tokens + tokensToAdd);
      this.lastRefill = now;
    }
  }

  consume(count = 1) {
    this.refill();
    if (this.tokens >= count) {
      this.tokens -= count;
      return true;
    }
    return false;
  }
}

// ------------------ Create buckets ------------------

// sales_order has its own bucket
const salesOrderBucket = new TokenBucket({ capacity: 10, refillRate: 5 });

// check_inventory & check_order share one bucket
const sharedInventoryOrderBucket = new TokenBucket({ capacity: 20, refillRate: 10 });

// ------------------ Queue Consumer ------------------
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

    try {
      // ------------------ Token selection ------------------
      let hasToken = false;

      if (queueName === 'sales_order') {
        hasToken = salesOrderBucket.consume(1);
        if (hasToken) {
          logger.consumer.info(`[${queueName}] Consumed 1 token. Tokens left: ${salesOrderBucket.tokens.toFixed(2)}`);
          console.log(`[${queueName}] Consumed 1 token. Tokens left: ${salesOrderBucket.tokens.toFixed(2)}`);
        }
      } else if (queueName === 'check_inventory' || queueName === 'check_order') {
        hasToken = sharedInventoryOrderBucket.consume(1);
        if (hasToken) {
          logger.consumer.info(`[SharedBucket:${queueName}] Consumed 1 token. Tokens left: ${sharedInventoryOrderBucket.tokens.toFixed(2)}`);
          console.log(`[SharedBucket:${queueName}] Consumed 1 token. Tokens left: ${sharedInventoryOrderBucket.tokens.toFixed(2)}`);
        }
      }

      // ------------------ No token available → respond failure ------------------
      if (!hasToken) {
        const failResponse = {
          state: "failure",
          responseMsg: "high system load",
          responseCode: 20
        };
        channel.sendToQueue(replyTo, Buffer.from(JSON.stringify(failResponse)), { correlationId });
        console.log(`[${queueName}] No token available → responded with high system load`);
        logger.consumer.info(`[${queueName}] No token available → responded with high system load`);
        channel.ack(msg);
        return;
      }

      // ------------------ Normal processing ------------------
      const erpResponse = await axios.post(handler.endpoint, payload);
      const response = { success: true, data: erpResponse.data };
      channel.sendToQueue(replyTo, Buffer.from(JSON.stringify(response)), { correlationId });
      channel.ack(msg);

    } catch (err) {
      const response = {
        success: false,
        error: err.response?.data || err.message
      };
      channel.sendToQueue(replyTo, Buffer.from(JSON.stringify(response)), { correlationId });
      channel.ack(msg);
    }
  });

  logger.consumer.info(`Listening on ${queueName}`);
}

// ------------------ Consumer Starter ------------------
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
