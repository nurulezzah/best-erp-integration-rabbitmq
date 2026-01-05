const pool = require('../db');
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
        // POST REQUEST TO BEST ERP
        // const response = await axios.post(
        //   'http://127.0.0.1:3000/checkorder',
        //   dsReq,
        //   { headers: { 'Content-Type': 'application/json' } }
        // );

        // const RABBIT_URL = 'amqp://ezzah:afm01@127.0.0.1:5672/';
        const RABBIT_URL = 'amqp://localhost';
        const connection = await amqp.connect(RABBIT_URL);
        const channel = await connection.createChannel();

        // Create a temporary exclusive queue for replies
        const { queue: replyQueue } = await channel.assertQueue('', { exclusive: true });
        const correlationId = uuidv4();

        return new Promise((resolve) => {
          // Listen for the response
          channel.consume(replyQueue, async (msg) => {
            if (msg.properties.correlationId === correlationId) {
              const raw = msg.content.toString();
              const response = JSON.parse(raw); 
              console.log("response",response.data);
              logger.downstream.info(`Response from BEST ERP: ${JSON.stringify(response.data, null, 2)}`);

              const rawRes = `
                UPDATE broku_co_downstream_output_raw
                SET rawresponse = $1,
                    response_date = $2
                WHERE uuid = $3;
              `;

              let rawResVal = [
                response.data,
                getCurrentDateTime(),
                outputRawUuid
              ];

              await pool.query(rawRes, rawResVal);


              const baseRes = `
                UPDATE broku_co_downstream_output_formatted
                SET state = $1,
                    responsecode = $2,
                    response_date = $3
                WHERE uuid = $4;
              `;

              let baseResVal = [
                response.data.state,
                response.data.responsecode,
                getCurrentDateTime(),
                outputFormattedUuid
              ];

              await pool.query(baseRes, baseResVal);


              const baseInputRes = `
                UPDATE broku_co_downstream_input_formatted
                SET state = $1,
                    responsecode = $2,
                    response_date = $3
                WHERE uuid = $4;
              `;

              let baseInputResVal = [
                response.data.state,
                response.data.responsecode,
                getCurrentDateTime(),
                formattedUuid
              ];

              await pool.query(baseInputRes, baseInputResVal);


              const rawInputRes = `
                UPDATE broku_co_downstream_input_raw
                SET rawresponse = $1,
                    response_date = $2
                WHERE uuid = $3;
              `;

              let rawInputResVal = [
                response.data,
                getCurrentDateTime(),
                rawUuid
              ];

              await pool.query(rawInputRes, rawInputResVal);

              resolve(response.data);
              setTimeout(() => {
                  connection.close();
              }, 500);
            }
          }, { noAck: true });

          // Send the request
          channel.sendToQueue(
              'check_order',
              Buffer.from(JSON.stringify(dsReq)),
              {
                  correlationId: correlationId,
                  replyTo: replyQueue
              }
          );
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