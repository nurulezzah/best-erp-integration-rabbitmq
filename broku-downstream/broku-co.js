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


async function checkOrderStatus(input){
  const appid = 60163222354;
  const servicetype = 'QUERY_SKU_SOURCE_ORDER';

  //insert raw data into db
  const query = `
      INSERT INTO broku_co_downstream_input_raw (rawdata)
      VALUES ($1::jsonb)
      RETURNING uuid;
  `;
  let values =[
      input
  ]
  let dbResult = await pool.query(query, values); // input is your JSON object
  const rawUuid = dbResult.rows[0].uuid;

  const query1 = `
      INSERT INTO broku_co_downstream_input_formatted (rawuuid,onlineOrderNumber)
      VALUES ($1,$2)
      RETURNING uuid;
  `;
  let values1 =[
    rawUuid,
    input.onlineOrderNumber
  ]
  let dbResult1 = await pool.query(query1, values1); // input is your JSON object
  const formattedUuid = dbResult1.rows[0].uuid;


  try{
    const query2 = `
      SELECT ordernumber
      FROM so_bizcontent_result
      WHERE onlineordernumber = $1
    `;

    const { rows } = await pool.query(query2, [input.onlineOrderNumber]);
    const orderNumbers = rows.map(r => r.ordernumber);

    if(orderNumbers.length===1){

      let dsReq = {
        "appId" : appid,
        "serviceType" : servicetype,
        "orderNumbers" : orderNumbers[0]
      }

      const query3 = `
        INSERT INTO broku_co_downstream_output_formatted (downstream_input_uuid,appid,servicetype,ordernumber)
        VALUES ($1,$2,$3,$4)
        RETURNING uuid;
      `;
      let values3 =[
        formattedUuid,
        dsReq.appId,
        dsReq.serviceType,
        dsReq.orderNumbers
      ];
      let dbResult3 = await pool.query(query3, values3); // input is your JSON object
      const outputFormattedUuid = dbResult3.rows[0].uuid;


      const query4 = `
        INSERT INTO broku_co_downstream_output_raw (downstream_output_uuid,rawdata)
        VALUES ($1, $2::jsonb)
        RETURNING uuid;
      `;
      let values4 =[
        outputFormattedUuid,
        dsReq
      ]
      let dbResult4 = await pool.query(query4, values4); // input is your JSON object
      const outputRawUuid = dbResult4.rows[0].uuid;


      try{
        // POST REQUEST TO RMQ

        const RABBIT_URL = 'amqp://'+config.RABBITMQ_USER+':'+config.RABBITMQ_PASS+'@'+config.RABBITMQ_HOST+':'+config.RABBITMQ_PORT+'/'+config.RABBITMQ_VHOST;
        // const RABBIT_URL = 'amqp://localhost';
        const connection = await amqp.connect(RABBIT_URL);
        const channel = await connection.createChannel();
        await channel.assertQueue('check_order', { durable: true });

        // Create a temporary exclusive queue for replies
        const { queue: replyQueue } = await channel.assertQueue('', { exclusive: true });
        const correlationId = uuidv4();

       return new Promise(async (resolve, reject) => {
            let timeoutHandle;
            let consumerTag;

            const cleanup = async () => {
                clearTimeout(timeoutHandle);
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
                  clearTimeout(timeoutHandle); // 

                  try {
                    const raw = msg.content.toString();
                    const response = JSON.parse(raw);
                    
                    logger.downstream.info(
                      `Response from BEST ERP: ${JSON.stringify(response.data, null, 2)}`
                    );

                    const rawRes = `
                      UPDATE broku_co_downstream_output_raw
                      SET rawresponse = $1,
                          response_date = $2
                      WHERE uuid = $3;
                    `;
                    await pool.query(rawRes, [
                      response.data,
                      getCurrentDateTime(),
                      outputRawUuid
                    ]);

                    const baseRes = `
                      UPDATE broku_co_downstream_output_formatted
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
                      UPDATE broku_co_downstream_input_formatted
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
                      UPDATE broku_co_downstream_input_raw
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

                    const rawRes = `
                      UPDATE broku_co_downstream_output_raw
                      SET rawresponse = $1,
                          response_date = $2
                      WHERE uuid = $3;
                    `;
                    await pool.query(rawRes, [
                      failResponse,
                      getCurrentDateTime(),
                      outputRawUuid
                    ]);

                    const baseRes = `
                      UPDATE broku_co_downstream_output_formatted
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
                      UPDATE broku_co_downstream_input_formatted
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
                      UPDATE broku_co_downstream_input_raw
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

          try {
            channel.sendToQueue(
              'check_order', 
              Buffer.from(JSON.stringify(dsReq)),
              { correlationId, replyTo: replyQueue,expiration: '38000' }
            );

            timeoutHandle = setTimeout(async () => {
              logger.downstream.error('RabbitMQ RPC timeout after 38 seconds');

              const failResponse = {
                state: 'failure',
                responsecode: 1,
                responsedate: getCurrentDateTime()
              };

              const rawRes = `
                UPDATE broku_co_downstream_output_raw
                SET rawresponse = $1,
                    response_date = $2
                WHERE uuid = $3;
              `;
              await pool.query(rawRes, [
                failResponse,
                getCurrentDateTime(),
                outputRawUuid
              ]);

              const baseRes = `
                UPDATE broku_co_downstream_output_formatted
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
                UPDATE broku_co_downstream_input_formatted
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
                UPDATE broku_co_downstream_input_raw
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

            const rawRes = `
              UPDATE broku_co_downstream_output_raw
              SET rawresponse = $1,
                  response_date = $2
              WHERE uuid = $3;
            `;
            await pool.query(rawRes, [
              failResponse,
              getCurrentDateTime(),
              outputRawUuid
            ]);

            const baseRes = `
              UPDATE broku_co_downstream_output_formatted
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
              UPDATE broku_co_downstream_input_formatted
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
              UPDATE broku_co_downstream_input_raw
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


      } catch (err) {
        logger.downstream.error('Error at post data to upstream:', err);
      } 

    }else{
      const responseFail ={
        "state" : "failure",
        "responsecode" : 1,
        "response date" : getCurrentDateTime()
      };

      const formattedInputRes = `
        UPDATE broku_co_downstream_input_formatted
        SET responsecode = $1,
            response_date = $2,
            state = $3
        WHERE uuid = $4;
      `;

        let formattedInputResVal = [
        responseFail.responsecode,
        getCurrentDateTime(),
        responseFail.state,
        formattedUuid
      ];

      await pool.query(formattedInputRes, formattedInputResVal);

      const rawInputRes = `
        UPDATE broku_co_downstream_input_raw
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

  }catch  {
    const responseFail ={
      "state" : "failure",
      "responsecode" : 1,
      "response date" : getCurrentDateTime()
    };

    const formattedInputRes = `
      UPDATE broku_co_downstream_input_formatted
      SET responsecode = $1,
          response_date = $2,
          state = $3
      WHERE uuid = $4;
    `;

      let formattedInputResVal = [
      responseFail.responsecode,
      getCurrentDateTime(),
      responseFail.state,
      formattedUuid
    ];

    await pool.query(formattedInputRes, formattedInputResVal);

    const rawInputRes = `
      UPDATE broku_co_downstream_input_raw
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
module.exports = { checkOrderStatus };