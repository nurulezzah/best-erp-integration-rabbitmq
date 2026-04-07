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

const appid = 60163222354;
const servicetype = 'QUERY_SIMPLE_LIST_INVENTORY_V2';


const RABBIT_URL = `amqp://${config.RABBITMQ_USER}:${config.RABBITMQ_PASS}@${config.RABBITMQ_HOST}:${config.RABBITMQ_PORT}/${config.RABBITMQ_VHOST}`;
let connection;
let channel;
let replyQueue;
const pendingRequests = new Map();

const buildFailure = (code = 1) => ({
  state: 'failure',
  responsecode: code,
  responsedate: getCurrentDateTime()
});

// Single function to update all downstream tables
async function updateAllTables(formattedUuid, rawUuid, responseObj, bool) {
  await pool.query(`
    UPDATE easystore_inv_downstream_output
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
    UPDATE easystore_inv_downstream_variants
    SET fail_response = $1,
        response_date = $2
    WHERE uuid = $3;
  `, [
    bool,
    responseObj.responsedate,
    formattedUuid
  ]);

  await pool.query(`
    UPDATE easystore_inv_downstream_input_raw
    SET rawresponse = $1,
        response_date = $2
    WHERE uuid = $3;
  `, [
    responseObj,
    responseObj.responsedate,
    rawUuid
  ]);
}

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
    SELECT inv_last_sync_time
    FROM last_sync_times
    WHERE inv_last_sync_time IS NOT NULL
    ORDER BY inv_last_sync_time DESC
    LIMIT 1;
  `);

  console.log('Last sync time from DB:', res.rows[0]?.inv_last_sync_time);
  let lastSync = res.rows[0]?.inv_last_sync_time;

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


async function initRMQ() {
  connection = await amqp.connect(RABBIT_URL);
  channel = await connection.createChannel();

  await channel.assertQueue('check_inventory', { durable: true });

  // Create ONE reply queue for all requests
  const q = await channel.assertQueue('', { exclusive: true });
  replyQueue = q.queue;

  // ONE consumer for all responses
  await channel.consume(
    replyQueue,
    async (msg) => {
      if (!msg) return;

      const correlationId = msg.properties.correlationId;
      const handler = pendingRequests.get(correlationId);

      if (!handler) return;

      pendingRequests.delete(correlationId);

      try {
        const response = JSON.parse(msg.content.toString());
        await handler.resolve(response);
      } catch (err) {
        await handler.reject(err);
      }
    },
    { noAck: true }
  );

  console.log('RMQ initialized');
}

async function fetchOrders(store,page) { 
  try {
    const response = await axios.get(store.get_inventory_url, {
      headers: {
        'EasyStore-Access-Token': store.token
      },
      params: {
        page: page,
        limit:5
      }
    });
    // logger.downstream.info(`Fetched orders from EasyStore ${store.name}: ${JSON.stringify(response.data)}`);
    return response.data;
  } catch (error) {
    console.error('[EasyStore] API Error:', error.response?.data || error.message);
    throw error;
  }

}
async function fetchEasyStoreOrders(store, manualMin = null, manualMax = null) {
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

    // Fetch first page to get page_count
    const firstPage = await fetchOrders(store, 1);
    const page_count = firstPage.page_count || 1; // fallback if page_count is missing

    for (let page = 1; page <= page_count; page++) {
      const res = page === 1 ? firstPage : await fetchOrders(store, page);

      console.log(`Fetched page ${page} for ${store.name}`);

      // Insert full response per page into raw table
      const rawInsert = await pool.query(`
        INSERT INTO easystore_inv_downstream_input_raw (rawdata)
        VALUES ($1)
        RETURNING uuid
      `, [JSON.stringify(res)]);

      const rawUuid = rawInsert.rows[0].uuid;

      console.log("res",res);
      // Process this page immediately
      await processOrders(res, rawUuid, store);

      console.log(`Inserted and processed page ${page}, uuid: ${rawUuid}`);
    }

    console.log(`Finished all pages for store ${store.name}`);

  } catch (error) {
    console.error('[EasyStore] API Error:', error.response?.data || error.message);
    throw error;
  }
}

async function processOrders(data, rawUuid, store) {

  const orders = data || [];
  const searchTerm = store.client;
  const clientRes = await pool.query(
    `SELECT * FROM client_data WHERE client ILIKE $1`,
    [searchTerm]
  );

  if (!clientRes.rows.length) {
    const failRes = buildFailure();
    await updateAllTables(null, rawUuid, failRes, 1);
    return;
  }

  const warehouse = clientRes.rows[0].warehouse;

  const rmqTasks = [];

  let products = orders.products || [];
  let formattedUuid = null;

  for (const product of products) {
    try {
      if (!product.variants || product.variants.length !== 1) {
        console.log(`Skipping product ${product.id} — invalid variants`);
        continue;
      }

      const variant = product.variants[0];

      // SKU check
      if (!variant?.sku || variant.sku.trim() === '') {
        console.log(`Skipping product ${product.id} — SKU missing/empty`);
        continue;
      }

      // --- Check if variant SKU already exists in downstream_variants ---
      const existingRes = await pool.query(
        `SELECT uuid FROM easystore_inv_downstream_variants WHERE sku = $1`,
        [variant.sku]
      );

      let formatted;
      if (existingRes.rows.length > 0) {
        // SKU exists → UPDATE
        const uuidToUpdate = existingRes.rows[0].uuid;
        const updateFields = {
          rawuuid: rawUuid,
          total_count: orders.total_count,
          page_count: orders.page_count,
          page: orders.page,
          variants_id: variant.id,
          product_id: variant.product_id,
          name: variant.name,
          price: parseFloat(variant.price),
          inventory_quantity: variant.inventory_quantity,
          inventory_management: variant.inventory_management
        };

        const setClause = Object.keys(updateFields)
          .map((key, idx) => `${key} = $${idx + 1}`)
          .join(", ");

        await pool.query(
          `UPDATE easystore_inv_downstream_variants SET ${setClause} WHERE uuid = $${Object.keys(updateFields).length + 1}`,
          [...Object.values(updateFields), uuidToUpdate]
        );

        // fetch the updated row
        const updatedRes = await pool.query(
          `SELECT * FROM easystore_inv_downstream_variants WHERE uuid = $1`,
          [uuidToUpdate]
        );
        formatted = updatedRes.rows[0];

      } else {
        // SKU does not exist → INSERT
        formatted = await dynamicInsert(
          pool,
          'easystore_inv_downstream_variants',
          {
            rawuuid: rawUuid,
            total_count: orders.total_count,
            page_count: orders.page_count,
            page: orders.page,
            variants_id: variant.id,
            product_id: variant.product_id,
            name: variant.name,
            sku: variant.sku,
            price: parseFloat(variant.price),
            inventory_quantity: variant.inventory_quantity,
            inventory_management: variant.inventory_management
          }
        );
      }

      if (!formatted) continue;

      formattedUuid = formatted.uuid;

      const smf = {
        appid: appid,
        servicetype: servicetype,
        sku: variant.sku,
        warehouse: warehouse
      };

      await dynamicInsert(
        pool,
        'easystore_inv_downstream_output',
        {
          downstream_input_uuid: formattedUuid,
          ...smf
        }
      );

      logger.downstream.info(`Downstream output for order ${JSON.stringify(smf)}`);
      console.log('Prepared SMF for RMQ:', smf);

      rmqTasks.push(sendToRMQ(formattedUuid, rawUuid, smf, store));

    } catch (err) {
      const failRes = buildFailure();
      await updateAllTables(formattedUuid, rawUuid, failRes, 1);
      console.error('Error processing product:', err.message);
    }
  }
    
  await Promise.all(rmqTasks);
}


async function sendToRMQ(formattedUuid, rawUuid, payload, store) {
  const correlationId = uuidv4();

  return new Promise((resolve) => {
    const timeout = setTimeout(async () => {
      console.error('RabbitMQ RPC timeout');
      logger.downstream.error('RabbitMQ RPC timeout');
      pendingRequests.delete(correlationId);

      const failRes = buildFailure();
      await updateAllTables(formattedUuid, rawUuid, failRes, 1);
      logger.downstream.info(`RMQ Failure: ${JSON.stringify(failRes)}`);
      resolve(failRes);
    }, 38000);

    pendingRequests.set(correlationId, {
      resolve: async (response) => {
        clearTimeout(timeout);

        let finalResponse;

        try {
          if (response.data.state === 'success') {
            await UpdateInventory(response.data.result, formattedUuid, store);
            finalResponse = { state: 'success', responsedate: getCurrentDateTime() };
          } else {
            finalResponse = buildFailure();
          }

          await updateAllTables(formattedUuid, rawUuid, finalResponse, null);
          logger.downstream.info(`Final Response: ${JSON.stringify(finalResponse)}`);
          resolve(finalResponse);

        } catch (err) {
          const failRes = buildFailure();
          await updateAllTables(formattedUuid, rawUuid, failRes, 1);
          logger.downstream.info(`Final Response: ${JSON.stringify(failRes)}`);
          resolve(failRes);
        }
      },
      reject: async () => {
        clearTimeout(timeout);
        const failRes = buildFailure();
        await updateAllTables(formattedUuid, rawUuid, failRes, 1);
        resolve(failRes);
      }
    });

    channel.sendToQueue(
      'check_inventory',
      Buffer.from(JSON.stringify(payload)),
      {
        correlationId,
        replyTo: replyQueue,
        expiration: '38000'
      }
    );
  });
}

async function UpdateInventory(data, formatteduuid,store) {

  let quantity = data.available;
    try {
      const getData = await pool.query(
        `SELECT variants_id, product_id FROM easystore_inv_downstream_variants WHERE uuid = $1`,
        [formatteduuid]
      );

      let putInv= await axios.put(store.put_inventory_url.replace('{product_id}', getData.rows[0].product_id), {
        "variants": [{
          "id": getData.rows[0].variants_id,
          "inventory_quantity": quantity
        }]
      },{
        headers: {
          'EasyStore-Access-Token': store.token,
          'Content-Type': 'application/json'
        }
      });
      // console.log('EasyStore Put Inventory Response:', putInv.data);
      logger.downstream.info(`EasyStore Put Inventory Response: ${JSON.stringify(putInv.data)}`);
      await pool.query(`
      UPDATE easystore_inv_downstream_variants
      SET inventory_quantity = $1,
      updated_date = $2
      WHERE uuid = $3
      RETURNING *
      `, 
      [putInv.data.variants[0].inventory_quantity, 
      getCurrentDateTime(), 
      formatteduuid]);

    }catch(err) {
      console.error('Error fetching variant for inventory update:', err.message);
      const failRes = buildFailure();
      await updateAllTables(formatteduuid, null, failRes, 1);
      return;
    }
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

async function shutdown() {
  try {

    if (channel) await channel.close();
    if (connection) await connection.close();
    await pool.end();

    process.exit(0); // force exit
  } catch (err) {
    console.error('Shutdown error:', err.message);
    process.exit(1);
  }
}

async function run(updatedMin = null, updatedMax = null) {
  const stores = config.EASYSTORES;
  if (!Array.isArray(stores)) throw new Error('EASYSTORES must be an array');

  for (const store of stores) {
    try {
      console.log(`\n=== Processing ${store?.name || store?.client || 'UNKNOWN STORE'} ===`);
      await fetchEasyStoreOrders(store, updatedMin, updatedMax);
    } catch (err) {
      logger.downstream.error(`[${store.name}] Sync failed: ${err.message}`);
    }
  }

  const fetchTime = lastSyncTime(getCurrentDateTime());
  await pool.query(`INSERT INTO last_sync_times (inv_last_sync_time) VALUES ($1)`, [fetchTime]);
}

const manualMin = process.argv[2];
const manualMax = process.argv[3];

async function scheduledRun() {
  console.log(getCurrentDateTime(), '=== Starting EasyStore Orders Job ===');

  try {
    await initRMQ();
    await run(manualMin, manualMax);
  } catch (err) {
    console.error('Scheduled run failed:', err.message);
  }

  console.log(getCurrentDateTime(), '=== Finished EasyStore Orders Job ===');
  await shutdown();
}
scheduledRun();
