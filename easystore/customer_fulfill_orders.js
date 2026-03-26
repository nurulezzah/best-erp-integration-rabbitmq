const pool = require('../db');
const path = require('path');
const loadConfig = require('../config/envLoader');

const configPath = path.resolve(__dirname, '../config/app.conf');
const config = loadConfig(configPath);
config.EASYSTORES = JSON.parse(config.EASYSTORES);
const axios = require('axios');
const logger = require('../logger'); 
const amqp = require('amqplib');
const { v4: uuidv4 } = require('uuid');

function getCurrentDateTime() {
  const now = new Date();
  const year = now.getFullYear();
  const month = String(now.getMonth() + 1).padStart(2, "0");
  const day = String(now.getDate()).padStart(2, "0");
  const hours = String(now.getHours()).padStart(2, "0");
  const minutes = String(now.getMinutes()).padStart(2, "0");
  const seconds = String(now.getSeconds()).padStart(2, "0");
  return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`;
}

function convertToMYT(utcTime) {
  if (!utcTime) return null;

  const date = new Date(utcTime);

  const options = {
    timeZone: 'Asia/Kuala_Lumpur', 
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false
  };

  const parts = new Intl.DateTimeFormat('en-GB', options).formatToParts(date);

  const obj = {};
  parts.forEach(p => obj[p.type] = p.value);

  return `${obj.year}-${obj.month}-${obj.day} ${obj.hour}:${obj.minute}:${obj.second}`;
}

// Helper to build failure object
const buildFailure = (code = 1) => ({
  state: 'failure',
  responsecode: code,
  responsedate: getCurrentDateTime()
});

// Single function to update all downstream tables
async function updateAllTables(formattedUuid, rawUuid, responseObj) {
  await pool.query(`
    UPDATE easystore_so_downstream_output
    SET state = $1,
        responsecode = $2,
        response_date = $3
    WHERE downstream_input_uuid = $4;
  `, [
    responseObj.state,
    responseObj.responsecode || null,
    responseObj.responsedate,
    formattedUuid
  ]);

  await pool.query(`
    UPDATE easystore_so_downstream_input_formatted
    SET post_status = $1,
        response_date = $2
    WHERE uuid = $3;
  `, [
    responseObj.state,
    responseObj.responsedate,
    formattedUuid
  ]);

  await pool.query(`
    UPDATE easystore_so_downstream_input_raw
    SET rawresponse = $1,
        response_date = $2
    WHERE uuid = $3;
  `, [
    responseObj,
    responseObj.responsedate,
    rawUuid
  ]);
}

function lastSyncTime(mytTime) {
  if (!mytTime) return null;

  const isoLike = mytTime.replace(' ', 'T'); 
  const date = new Date(`${isoLike}+08:00`); 

  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");
  const hours = String(date.getHours()).padStart(2, "0");
  const minutes = String(date.getMinutes()).padStart(2, "0");
  const seconds = String(date.getSeconds()).padStart(2, "0");

  return `${year}-${month}-${day}T${hours}:${minutes}:${seconds}+08:00`;
}

async function getLastSyncTimeFromDB() {
  const res = await pool.query(`
    SELECT last_sync_time
    FROM last_sync_times
    WHERE last_sync_time IS NOT NULL
    ORDER BY last_sync_time DESC
    LIMIT 1;
  `);

  console.log('Last sync time from DB:', res.rows[0]?.last_sync_time);
  let lastSync = res.rows[0]?.last_sync_time;

  if (!lastSync) {
     // Table is empty → fallback to TODAY at 12:00:00 MYT
    const now = new Date();
    const year = now.getFullYear();
    const month = String(now.getMonth() + 1).padStart(2, '0');
    const day = String(now.getDate()).padStart(2, '0');
    const fallback = `${year}-${month}-${day} 12:00:00`; // today at 12:00
    lastSync = lastSyncTime(fallback); // converts to "YYYY-MM-DDTHH:mm:ss+08:00"
  }

  return lastSync;
}

async function fetchEasyStoreOrders(store,manualMin = null, manualMax = null) {
  try {
    let updatedAtMin;
    let updatedAtMax;

    if (manualMin && manualMax) {
      updatedAtMin = manualMin;
      updatedAtMax = manualMax;
    } else {
      const lastSync = await getLastSyncTimeFromDB();
      const now = getCurrentDateTime();

      updatedAtMin = lastSync;
      updatedAtMax = lastSyncTime(now);
    }
    
    logger.downstream.info(`Fetching orders updated between ${updatedAtMin} and ${updatedAtMax}`);
    console.log(`Fetching orders updated between ${updatedAtMin} and ${updatedAtMax}`);
    const response = await axios.get(store.url, {
      headers: {
        'EasyStore-Access-Token': store.token
      },
      params: {
        financial_status: 'paid',
        updated_at_min: updatedAtMin,
        updated_at_max: updatedAtMax,
        fulfillment_status: 'fulfilled'
      }
    });

    logger.downstream.info(`Fetched orders from EasyStore: ${JSON.stringify(response.data.orders)}`);
    console.log('Fetched orders from EasyStore', response.data.orders);

    const rawInsert = await pool.query(`
      INSERT INTO easystore_so_downstream_input_raw (rawdata)
      VALUES ($1)
      RETURNING *
    `, [JSON.stringify(response.data)]);

    const rawUuid = rawInsert.rows[0].uuid;
    return { data: response.data, rawUuid };

  } catch (error) {
    console.error('[EasyStore] API Error:', error.response?.data || error.message);
    throw error;
  }
}

async function processOrders(data, rawUuid, store) {

  const orders = data.orders || [];
  if (!orders.length) return;
  const searchTerm = store.client;
  const clientRes = await pool.query(
    `SELECT * FROM client_data WHERE client ILIKE $1`,
    [searchTerm]
  );

  if (!clientRes.rows.length) {
    throw new Error('Client not found');
  }

  const shop = clientRes.rows[0].shop;

  const rmqTasks = [];

  for (const order of orders) {

    let paid_at= convertToMYT(order.paid_at);
    let order_created = convertToMYT(order.created_at);

    try{
      if ( order.fulfillments &&  order.fulfillments.length === 1) {
        try {
          const formatted = await dynamicInsert(
            pool,
            'easystore_so_downstream_input_formatted',
            {
              rawuuid: rawUuid,
              ...order,
              subtotal_price: parseFloat(order.subtotal_price),
              total_discount: parseFloat(order.total_discount),
              total_amount: parseFloat(order.total_amount),
              total_price: parseFloat(order.total_price),
              total_shipping: parseFloat(order.total_shipping),
              customer: JSON.stringify(order.customer),
              line_items: JSON.stringify(order.line_items),
              shipping_address: JSON.stringify(order.shipping_address),
              fulfillments: JSON.stringify(order.fulfillments),
              order_created: order_created,
              paid_at: paid_at,
              note: order.note
            }
          );

          if (!formatted) continue;

          const formattedUuid = formatted.uuid;

          const sideInsertTasks = [];

          if (order.customer) {
            sideInsertTasks.push(
              dynamicInsert(pool, 'easystore_so_customer', {
                downstream_input_uuid: formattedUuid,
                ...order.customer,
                total_spent: parseFloat(order.customer.total_spent)
              })
            );
          }

          if (order.shipping_address) {
            sideInsertTasks.push(
              dynamicInsert(pool, 'easystore_so_shipping_address', {
                downstream_input_uuid: formattedUuid,
                ...order.shipping_address
              })
            );
          }

          if (order.fulfillments) {
            sideInsertTasks.push(
              dynamicInsert(pool, 'easystore_so_fulfillments', {
                downstream_input_uuid: formattedUuid,
                ...order.fulfillments[0]
              })
            );
          }

          await Promise.all(sideInsertTasks);

          const skuList = [];

          const skuTasks = order.line_items.map(item => {

            skuList.push({
              sku: item.sku,
              payAmount: parseFloat(order.total_amount),
              paymentPrice: parseFloat(item.price),
              quantity: item.quantity
            });

            return Promise.all([
              dynamicInsert(pool, 'easystore_so_line_items', {
                downstream_input_uuid: formattedUuid,
                ...item,
                price: parseFloat(item.price),
                total_discount: parseFloat(item.total_discount),
                total_tax: parseFloat(item.total_tax),
                total_amount: parseFloat(item.total_amount),
                fulfillment_service: JSON.stringify(item.fulfillment_service)
              }),

              dynamicInsert(pool, 'easystore_so_output_sku', {
                downstream_input_uuid: formattedUuid,
                sku: item.sku,
                payamount: parseFloat(order.total_amount),
                quantity: item.quantity
              })
            ]);
          });

          await Promise.all(skuTasks);

          const smf = {
            appid: 60163222354,
            servicetype: 'CREATE_SALES_ORDER',
            shop: shop,
            onlineordernumber: `${store.code}${formatted.number}`,
            trackingnumber: order.fulfillments[0].tracking_number || null,
            carrier: order.fulfillments[0].tracking_company || null,
            paymentmethod: "PAY_ONLINE",
            codpayamount: 0.00,
            paytime: paid_at,
            skuList: skuList,
            receivername: [
              order.shipping_address?.first_name,
              order.shipping_address?.last_name
            ].filter(Boolean).join(' '),
            receiverphone: order.shipping_address?.phone,
            receiveraddress: [
              order.shipping_address?.address1,
              order.shipping_address?.address2
            ].filter(Boolean).join(', '),
            receiverpostcode: order.shipping_address?.zip,
            receivercity: order.shipping_address?.city,
            receiverprovince: order.shipping_address?.province,
            receivercountry: 'MY'
          };

          await dynamicInsert(pool, 'easystore_so_downstream_output', {
            downstream_input_uuid: formattedUuid,
            ...smf,
            sku: JSON.stringify(smf.skuList)
          });

          logger.downstream.info(
            `Downstream output for order ${JSON.stringify(smf)}`
          );

          console.log('Prepared SMF for RMQ:', smf);
          rmqTasks.push(
            sendToRMQ(formattedUuid, rawUuid,smf)
          );

        } catch (err) {
          logger.downstream.error(`Order ${order.id} failed:`, err.message);
        }
      }
    } catch(err) {
      console.error('Error processing fulfillments:', err.message);
      const failRes = buildFailure(2);
        await updateAllTables(formattedUuid, rawUuid, failRes);
    }

  }

  if (rmqTasks.length) {
    const results = await Promise.allSettled(rmqTasks);
    const valuesOnly = results
      .filter(r => r.status === 'fulfilled')
      .map(r => r.value);
  }
}

// Main function to send data to RMQ and wait for response
async function sendToRMQ(formattedUuid, rawUuid, payload) {
  const RABBIT_URL = `amqp://${config.RABBITMQ_USER}:${config.RABBITMQ_PASS}@${config.RABBITMQ_HOST}:${config.RABBITMQ_PORT}/${config.RABBITMQ_VHOST}`;

  const connection = await amqp.connect(RABBIT_URL);
  const channel = await connection.createChannel();

  await channel.assertQueue('sales_order', { durable: true });
  const { queue: replyQueue } = await channel.assertQueue('', { exclusive: true });
  const correlationId = uuidv4();

  let timeoutHandle;
  let consumerTag;

  const cleanup = async () => {
    try {
      if (timeoutHandle) clearTimeout(timeoutHandle);
      if (consumerTag) await channel.cancel(consumerTag);
      await channel.close();
      await connection.close();
    } catch (err) {
      logger.downstream.error(`[Cleanup error: ${err.message}`);
    }
  };

  return new Promise(async (resolve) => {
    try {

      const consumeResult = await channel.consume(
        replyQueue,
        async (msg) => {
          if (!msg) return;
          if (msg.properties.correlationId !== correlationId) return;

          clearTimeout(timeoutHandle);

          let finalResponse;
          try {
            const raw = msg.content.toString();
            const response = JSON.parse(raw);

            console.log('Downstream output: ', response);
            logger.downstream.info(`Response to client: ${JSON.stringify(response)}`);

            if (response.data.state === 'success') {
              finalResponse = { state: 'success', responsedate: getCurrentDateTime() };
            } else {
              finalResponse = buildFailure();
            }

            await updateAllTables(formattedUuid, rawUuid, finalResponse);

            resolve(finalResponse);

          } catch (err) {
            const failRes = buildFailure();
            logger.downstream.info(`Error processing consumer response: ${JSON.stringify(failRes)}`);
            await updateAllTables(formattedUuid, rawUuid, failRes);
            resolve(failRes);
          } finally {
            await cleanup();
          }
        },
        { noAck: true }
      );

      consumerTag = consumeResult.consumerTag;

      channel.sendToQueue(
        'sales_order',
        Buffer.from(JSON.stringify(payload)),
        { correlationId, replyTo: replyQueue, expiration: '38000' }
      );

      timeoutHandle = setTimeout(async () => {
        console.error('RabbitMQ RPC timeout after 38 seconds');
        const failRes = buildFailure();
        await updateAllTables(formattedUuid, rawUuid, failRes);
        await cleanup();
        resolve(failRes);
      }, 38000);

    } catch (err) {
      console.error("Failed to send to queue:", err.message);
      const failRes = buildFailure(21);
      await updateAllTables(formattedUuid, rawUuid, failRes);
      await cleanup();
      resolve(failRes);
    }
  });
}

async function run(updatedMin = null, updatedMax = null) {
  let stores = config.EASYSTORES;
  if (!Array.isArray(stores)) {
    throw new Error('EASYSTORES must be an array');
  }

  for (const store of stores) {
    try {
      console.log(`\n=== Processing ${store?.name || store?.client || 'UNKNOWN STORE'} ===`);

      const { data, rawUuid } = await fetchEasyStoreOrders(store,updatedMin,updatedMax);
      await processOrders(data, rawUuid, store);
    } catch (err) {
      logger.downstream.error(
        `[${store.name}] Sync failed: ${err.message}`
      );
    }
  }
  const fetchTime = lastSyncTime(getCurrentDateTime());

  await pool.query(`
    INSERT INTO last_sync_times (last_sync_time)
    VALUES ($1)
  `, [fetchTime]);

  // await processOrders(data, rawUuid, store);
}

const tableColumnsCache = {};

async function getTableColumns(pool, tableName) {
  if (!tableColumnsCache[tableName]) {
    const res = await pool.query(
      `SELECT column_name FROM information_schema.columns WHERE table_name = $1`,
      [tableName]
    );
    tableColumnsCache[tableName] = res.rows.map(r => r.column_name);
  }
  return tableColumnsCache[tableName];
}

async function dynamicInsert(pool, tableName, data) {
  const columns = await getTableColumns(pool, tableName);

  // filter keys that exist in the table
  const filtered = Object.keys(data)
    .filter(key => columns.includes(key))
    .reduce((obj, key) => {
      obj[key] = data[key];
      return obj;
    }, {});

  if (Object.keys(filtered).length === 0) {
    logger.upstream.info(`No matching columns for ${tableName}, skipping`);
    return null;
  }

  const fields = Object.keys(filtered).join(',');
  const placeholders = Object.keys(filtered)
    .map((_, i) => `$${i + 1}`)
    .join(',');

  const values = Object.values(filtered);

  const q = `INSERT INTO ${tableName} (${fields}) VALUES (${placeholders}) RETURNING *;`;
  const res = await pool.query(q, values);
  return res.rows[0]; // return full inserted row
}

const manualMin = process.argv[2];
const manualMax = process.argv[3];

async function scheduledRun() {
  console.log(getCurrentDateTime(), '=== Starting EasyStore Orders Job ===');

  try {
    await run(manualMin, manualMax);
  } catch (err) {
    console.error('Scheduled run failed:', err.message);
  }

  console.log(getCurrentDateTime(), '=== Finished EasyStore Orders Job ===');
}

scheduledRun();
