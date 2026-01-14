const amqp = require('amqplib');
const path = require('path');
const loadConfig = require('../config/envLoader');

const configPath = path.resolve(__dirname, '../config/app.conf');
const config = loadConfig(configPath);

const axios = require('axios');

const RABBIT_URL = config.RABBITMQ_URL;
// const RABBIT_URL = `amqp://localhost:5672`;
const QUEUE_HANDLERS = {
  sales_order: {
    endpoint: config.UPSTREAM_URL+config.ERP_SO_ENDPOINT
  },
  check_inventory: {
    endpoint: config.UPSTREAM_URL+config.ERP_INVENTORY_ENDPOINT
  },
  check_order: {
    endpoint: config.UPSTREAM_URL+config.ERP_CO_ENDPOINT
  }
};



async function consumeQueue(channel, queueName, handler) {
  await channel.assertQueue(queueName, { durable: true });

  channel.consume(queueName, async (msg) => {
    if (!msg) return;
    console.log("queuename=", queueName);
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
      console.log(`Processed ${queueName}:`, response);
      
    } catch (err) {
      response = {
        success: false,
        error: err.response?.data || err.message
      };

      console.log(`Error processing ${queueName}:`, response);
    }

    channel.sendToQueue(
      replyTo,
      Buffer.from(JSON.stringify(response)),
      { correlationId }
    );

    channel.ack(msg);
  });

  console.log(`Listening on ${queueName}`);
}

async function start() {
  console.log('Connecting to RabbitMQ at', RABBIT_URL);
  const connection = await amqp.connect(RABBIT_URL);
  const channel = await connection.createChannel();

  channel.prefetch(1);

  for (const [queueName, handler] of Object.entries(QUEUE_HANDLERS)) {
    await consumeQueue(channel, queueName, handler);
  }
}

start().catch(console.error);





