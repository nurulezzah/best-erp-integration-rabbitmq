const pool = require('../db');
const path = require('path');
const loadConfig = require('../config/envLoader');

const configPath = path.resolve(__dirname, '../config/app.conf');
const config = loadConfig(configPath);

const axios = require('axios');
const logger = require('../logger'); 
const { v4: uuidv4 } = require('uuid');
const amqp = require('amqplib');

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

async function processReq(input){
  const appid = 60163222354;
  const servicetype = 'CREATE_SALES_ORDER';

  const query = `
      INSERT INTO broku_so_downstream_input_raw (rawdata)
      VALUES ($1::jsonb)
      RETURNING uuid;
  `;
  let values =[
      input
  ]
  let dbResult = await pool.query(query, values); 
  const rawUuid = dbResult.rows[0].uuid;

  try {

      if (!input.client || input.client.trim() === "") {
        throw new Error("Client cannot be empty");
      }
      const searchTerm = `%${input.client}%`;
      const query = `
      SELECT *
      FROM client_data
      WHERE client ILIKE $1
      `;

      const result = await pool.query(query, [searchTerm]);
      
      const shop = JSON.stringify(result.rows[0].shop);

      const lowercaseInput = toLowerCaseKeys(input);

      const getFormatted = await dynamicInsert(pool, 'broku_so_downstream_input_formatted', {
          rawuuid: rawUuid,
          ...lowercaseInput,
          sku: JSON.stringify(lowercaseInput.skulist)
      });

      const formattedUuid = getFormatted.uuid;

      if (Array.isArray(lowercaseInput.skulist)) {
          for (const sku of lowercaseInput.skulist) {
          await pool.query(
              `
              INSERT INTO broku_so_downstream_input_formatted_sku
              (downstream_input_uuid, onlineordernumber, sku, payamount, quantity)
              VALUES ($1, $2, $3, $4, $5);
              `,
              [
              formattedUuid,
              lowercaseInput.onlineordernumber,
              sku.sku,
              parseFloat(sku.payamount) || 0,
              sku.quantity || 0
              ]
          );
          }
      }

      //need to retrieve the sku by order number
      const getSku = await pool.query(
          `SELECT * FROM broku_so_downstream_input_formatted_sku WHERE onlineordernumber = $1`,
          [lowercaseInput.onlineordernumber]
      );

      let getSkuList = getSku.rows
      .filter(row => row.downstream_input_uuid === formattedUuid) // only keep matching rows
      .map(({ downstream_input_uuid, created_date, ...rest }) => rest); // then remove unwanted keys


      // INSERT INTO DOWNSTREAM OUTPUT TABLE
      const getOutputFormatted = await dynamicInsert(pool, 'broku_so_downstream_output_formatted', {
          downstream_input_uuid: formattedUuid,
          ...lowercaseInput,
          sku: JSON.stringify(getSkuList),
          appid:appid,
          servicetype:servicetype,
          shop:JSON.parse(shop),
          paymentmethod:"PAY_ONLINE",
          codpayamount:0
      });

      const outputFormattedUuid = getOutputFormatted.uuid;
      delete getOutputFormatted.uuid;
      delete getOutputFormatted.downstream_input_uuid;
      delete getOutputFormatted.state;
      delete getOutputFormatted.responsecode;
      delete getOutputFormatted.response_date;
      delete getOutputFormatted.created_date;


      getOutputFormatted.skulist = getOutputFormatted.sku;
      delete getOutputFormatted.sku;

      const query2 = `
          INSERT INTO broku_so_downstream_output_raw (rawdata, downstream_output_uuid)
          VALUES ($1::jsonb,$2)
          RETURNING *;
      `;
      let values2 =[
          getOutputFormatted,
          outputFormattedUuid
      ]
      let outputRaw = await pool.query(query2, values2);

      logger.downstream.info(`Request to Upstream: ${JSON.stringify(getOutputFormatted, null, 2)}`);

      try{
        // POST REQUEST TO RMQ

        const RABBIT_URL = 'amqp://'+config.RABBITMQ_USER+':'+config.RABBITMQ_PASS+'@'+config.RABBITMQ_HOST+':'+config.RABBITMQ_PORT+'/'+config.RABBITMQ_VHOST;
        // const RABBIT_URL = 'amqp://localhost';
        const connection = await amqp.connect(RABBIT_URL);
        const channel = await connection.createChannel();
        await channel.assertQueue('sales_order', { durable: true });
        
        // Create a temporary exclusive queue for replies
        const { queue: replyQueue } = await channel.assertQueue('', { exclusive: true });
        const correlationId = uuidv4();

        return new Promise(async (resolve) => {
          let consumerTag;
          let timeoutHandle;

          const cleanup = async () => {
            try {
              if (timeoutHandle) clearTimeout(timeoutHandle);
              if (consumerTag) await channel.cancel(consumerTag);
              await channel.close();
              await connection.close();
            } catch (err) {
              console.error("Cleanup error:", err.message);
            }
          };

          const consumeResult = await channel.consume(
            replyQueue,
            async (msg) => {
              if (!msg) return;

              if (msg.properties.correlationId === correlationId) {
                clearTimeout(timeoutHandle); 

                try {
                  const raw = msg.content.toString();
                  const response = JSON.parse(raw);
                  
                  logger.downstream.info(
                    `Response from BEST ERP: ${JSON.stringify(response.data, null, 2)}`
                  );

                  // ---- DB Updates ----

                  const rawRes = `
                    UPDATE broku_so_downstream_output_raw
                    SET rawresponse = $1,
                        response_date = $2
                    WHERE uuid = $3;
                  `;
                  await pool.query(rawRes, [
                    response.data,
                    getCurrentDateTime(),
                    outputRaw.rows[0].uuid
                  ]);

                  const baseRes = `
                    UPDATE broku_so_downstream_output_formatted
                    SET state = $1,
                        responsecode = $2,
                        response_date = $3
                    WHERE uuid = $4;
                  `;
                  await pool.query(baseRes, [
                    response.data.state,
                    response.data.responsecode,
                    getCurrentDateTime(),
                    outputFormattedUuid
                  ]);

                  const baseInputRes = `
                    UPDATE broku_so_downstream_input_formatted
                    SET state = $1,
                        responsecode = $2,
                        response_date = $3
                    WHERE uuid = $4;
                  `;
                  await pool.query(baseInputRes, [
                    response.data.state,
                    response.data.responsecode,
                    getCurrentDateTime(),
                    formattedUuid
                  ]);

                  const rawInputRes = `
                    UPDATE broku_so_downstream_input_raw
                    SET rawresponse = $1,
                        response_date = $2
                    WHERE uuid = $3;
                  `;
                  await pool.query(rawInputRes, [
                    response.data,
                    getCurrentDateTime(),
                    rawUuid
                  ]);

                  resolve(response.data);

                } catch (err) {
                  console.error("Error processing consumer response:", err.message);

                  const failResponse = {
                    state: 'failure',
                    responsecode: 1,
                    responsedate: getCurrentDateTime()
                  };

                  // ---- KEEP rawRes ----
                  const rawRes = `
                    UPDATE broku_so_downstream_output_raw
                    SET rawresponse = $1,
                        response_date = $2
                    WHERE uuid = $3;
                  `;
                  await pool.query(rawRes, [
                    failResponse,
                    getCurrentDateTime(),
                    outputRaw.rows[0].uuid
                  ]);

                  const baseRes = `
                    UPDATE broku_so_downstream_output_formatted
                    SET state = $1,
                        responsecode = $2,
                        response_date = $3
                    WHERE uuid = $4;
                  `;
                  await pool.query(baseRes, [
                    'failure',
                    1,
                    getCurrentDateTime(),
                    outputFormattedUuid
                  ]);

                  const baseInputRes = `
                    UPDATE broku_so_downstream_input_formatted
                    SET state = $1,
                        responsecode = $2,
                        response_date = $3
                    WHERE uuid = $4;
                  `;
                  await pool.query(baseInputRes, [
                    'failure',
                    1,
                    getCurrentDateTime(),
                    formattedUuid
                  ]);

                  const rawInputRes = `
                    UPDATE broku_so_downstream_input_raw
                    SET rawresponse = $1,
                        response_date = $2
                    WHERE uuid = $3;
                  `;
                  await pool.query(rawInputRes, [
                    failResponse,
                    getCurrentDateTime(),
                    rawUuid
                  ]);

                  resolve(failResponse);

                } finally {
                  await cleanup();
                }
              }
            },
            { noAck: true }
          );

          consumerTag = consumeResult.consumerTag;

          // ---- Send request to queue ----
          try {
            channel.sendToQueue(
              'sales_order', // queue name
              Buffer.from(JSON.stringify(getOutputFormatted)),
              { correlationId, replyTo: replyQueue,expiration: '38000' }
            );

            // START 38-SECOND TIMEOUT ONLY AFTER SEND SUCCESS
            timeoutHandle = setTimeout(async () => {
              logger.downstream.error('RabbitMQ RPC timeout after 38 seconds');

              const failResponse = {
                state: 'failure',
                responsecode: 1,
                responsedate: getCurrentDateTime()
              };

              // ---- KEEP rawRes ----
              const rawRes = `
                UPDATE broku_so_downstream_output_raw
                SET rawresponse = $1,
                    response_date = $2
                WHERE uuid = $3;
              `;
              await pool.query(rawRes, [
                failResponse,
                getCurrentDateTime(),
                outputRaw.rows[0].uuid
              ]);

              const baseRes = `
                UPDATE broku_so_downstream_output_formatted
                SET state = $1,
                    responsecode = $2,
                    response_date = $3
                WHERE uuid = $4;
              `;
              await pool.query(baseRes, [
                'failure',
                1,
                getCurrentDateTime(),
                outputFormattedUuid
              ]);

              const baseInputRes = `
                UPDATE broku_so_downstream_input_formatted
                SET state = $1,
                    responsecode = $2,
                    response_date = $3
                WHERE uuid = $4;
              `;
              await pool.query(baseInputRes, [
                'failure',
                1,
                getCurrentDateTime(),
                formattedUuid
              ]);

              const rawInputRes = `
                UPDATE broku_so_downstream_input_raw
                SET rawresponse = $1,
                    response_date = $2
                WHERE uuid = $3;
              `;
              await pool.query(rawInputRes, [
                failResponse,
                getCurrentDateTime(),
                rawUuid
              ]);

              await cleanup();
              resolve(failResponse);

            }, 38000); // 38 seconds

          } catch (err) {
            console.error("Failed to send to queue:", err.message);

            const failResponse = {
              state: 'failure',
              responsecode: 1,
              responsedate: getCurrentDateTime()
            };

            // ---- ORIGINAL FAILURE HANDLING (UNCHANGED) ----
            const rawRes = `
              UPDATE broku_so_downstream_output_raw
              SET rawresponse = $1,
                  response_date = $2
              WHERE uuid = $3;
            `;
            await pool.query(rawRes, [
              failResponse,
              getCurrentDateTime(),
              outputRaw.rows[0].uuid
            ]);

            const baseRes = `
              UPDATE broku_so_downstream_output_formatted
              SET state = $1,
                  responsecode = $2,
                  response_date = $3
              WHERE uuid = $4;
            `;
            await pool.query(baseRes, [
              'failure',
              1,
              getCurrentDateTime(),
              outputFormattedUuid
            ]);

            const baseInputRes = `
              UPDATE broku_so_downstream_input_formatted
              SET state = $1,
                  responsecode = $2,
                  response_date = $3
              WHERE uuid = $4;
            `;
            await pool.query(baseInputRes, [
              'failure',
              1,
              getCurrentDateTime(),
              formattedUuid
            ]);

            const rawInputRes = `
              UPDATE broku_so_downstream_input_raw
              SET rawresponse = $1,
                  response_date = $2
              WHERE uuid = $3;
            `;
            await pool.query(rawInputRes, [
              failResponse,
              getCurrentDateTime(),
              rawUuid
            ]);

            await cleanup();
            resolve(failResponse);
          }

        });

      }catch (err) {
        logger.downstream.error('Error at post data to upstream:', err);
      } 

  } catch (err) {
      //return fail to find matching client in db
      logger.downstream.error('Error at check if client exist:', err);
      const responseFail ={
        "state" : "failure",
        "responsecode" : 1,
        "response date" : getCurrentDateTime()
      };

      const rawInputRes = `
        UPDATE broku_so_downstream_input_raw
        SET rawresponse = $1,
            response_date = $2
        WHERE uuid = $3;
      `;

      let rawInputResVal = [
        responseFail,
        getCurrentDateTime(),
        rawUuid
      ];

      await pool.query(rawInputRes, rawInputResVal);
      return await responseFail;
  } 
    
}

function toLowerCaseKeys(obj) {
  if (Array.isArray(obj)) {
    // for arrays, map each element
    return obj.map(item => toLowerCaseKeys(item));
  } else if (obj !== null && typeof obj === 'object') {
    // for objects, reduce keys
    return Object.keys(obj).reduce((acc, key) => {
      acc[key.toLowerCase()] = toLowerCaseKeys(obj[key]);
      return acc;
    }, {});
  }
  // primitives: return as is
  return obj;
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



module.exports = { processReq };
