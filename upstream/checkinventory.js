const pool = require('../db');
const path = require('path');
const loadConfig = require('../config/envLoader');

const configPath = path.resolve(__dirname, '../config/app.conf');
const config = loadConfig(configPath);

const crypto = require("crypto");
const axios = require('axios');
const FormData = require('form-data');
const logger = require('../logger'); 


const appSecret = "9ced6df12e6ebcba54b2877677640165";
const timestamp = Date.now(); //miliseconds

// function to create MD5 hash
function md5Hash(str) {
  return crypto.createHash("md5").update(str).digest("hex");
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


async function checkOrderStatus(input) {

    //insert raw data into db
    const query = `
        INSERT INTO inv_upstream_input_raw (rawdata)
        VALUES ($1::jsonb)
        RETURNING uuid;
    `;
      let values =[
        input
    ]

    let dbResult = await pool.query(query, values); 

    const rawUuid = dbResult.rows[0].uuid;
    
    
    const newinput =toLowerCaseKeys(input);
    const query2 = `
        INSERT INTO inv_upstream_input_formatted
        (rawuuid, appid, servicetype, sku, warehouse)
        VALUES ($1,$2,$3,$4::jsonb,$5)
        RETURNING *;
    `;

    const value2 = [
        rawUuid,
        newinput.appid,
        newinput.servicetype,
         JSON.stringify(newinput.sku),
        newinput.warehouse    ]
    let dbResult2 = await pool.query(query2, value2); 
    
    const formattedUuid = dbResult2.rows[0].uuid;
    const page = 1;
    const pageSize = 50;
    
    const query3 = `
        INSERT INTO inv_biz_param
        (skulist, warehouse, page, pagesize)
        VALUES ($1::json,$2,$3,$4)
        RETURNING uuid;
    `;

    const value3 = [
        JSON.stringify(newinput.sku),
        newinput.warehouse,
        page,
        pageSize
    ]
    let dbResult3 = await pool.query(query3, value3); 

    const bizParamUuid = dbResult3.rows[0].uuid;

    const bizParam = {
        "skuList" : newinput.sku,
        "warehouse" : newinput.warehouse,
        "page": page,
        "pageSize" : pageSize
    };


    let concatOrder =  "".concat("appId=", newinput.appid,"bizParam=", JSON.stringify(bizParam),"serviceType=", newinput.servicetype,"timestamp=", timestamp,appSecret);

    let sign = md5Hash(concatOrder);

    let baseReq = {
        "appId" : newinput.appid,
        "bizParam" : JSON.stringify(bizParam),
        "serviceType" : newinput.servicetype,
        "timestamp" : timestamp,
        "sign" : sign
    }

    const query4 = `
        INSERT INTO inv_base_req
        (uuid_upstream, uuid_bizparam, appid, bizparam, servicetype, timestamp, sign)
        VALUES ($1,$2,$3,$4::json,$5,$6,$7)
        RETURNING uuid;
    `;

    const value4 = [
        formattedUuid,
        bizParamUuid,
        baseReq.appId,
        baseReq.bizParam,
        baseReq.serviceType,
        baseReq.timestamp,
        baseReq.sign
    ];

    
    let dbResult4 = await pool.query(query4, value4);

    const baseReqUuid = dbResult4.rows[0].uuid;

    try {

        // prepare form-data
        const form = new FormData();
        form.append('appId', baseReq.appId);
        form.append('serviceType', baseReq.serviceType);
        form.append('sign', baseReq.sign);
        form.append('bizParam', baseReq.bizParam); // stringify JSON
        form.append('timestamp', baseReq.timestamp);
        form.append('appSecret', appSecret);

        const safeLog = {
            appId: baseReq.appId,
            serviceType: baseReq.serviceType,
            sign: baseReq.sign,
            bizParam: JSON.parse(baseReq.bizParam),
            timestamp: baseReq.timestamp,
            appSecret: appSecret
        };

        logger.upstream.info(`Request to BEST ERP: ${JSON.stringify(safeLog, null, 2)}`);

        // POST REQUEST TO BEST ERP
        const response = await axios.post(
        config.ERP_INVENTORY_URL, // replace with ERP URL
        // 'http://localhost:3001/api/v1/salesOrder',

        form,
        { headers: form.getHeaders() }
        );


        logger.upstream.info(`Response from BEST ERP: ${JSON.stringify(response.data, null, 2)}`);

        if(response.data.state == "success") // (0 && 0) || ( 0 && 1)  
        {
            let baseRes = `
                UPDATE inv_base_req
                SET state = $1,
                    errorcode = $2,
                    errormsg = $3,
                    bizcontent = $4::jsonb,
                    response_date = $5
                WHERE uuid = $6;
                `;

            let baseResVal = [
                response.data.state,
                response.data.errorCode,
                response.data.errorMsg,
                response.data.bizContent,
                getCurrentDateTime(),
                baseReqUuid
            ];

            await pool.query(baseRes, baseResVal);



            if (response?.data?.bizContent){

                const bizContentState = JSON.parse(response.data.bizContent);

                if(bizContentState.state == "success"){ // 0 && 0

                    const bizContentResult = toLowerCaseKeys(bizContentState.result[0]);

                    await dynamicInsert(pool, 'inv_biz_content_result', {
                        base_req_uuid: baseReqUuid,
                        ...bizContentResult
                    });


                    //create SMF RESPONSE
                    const formatted_res = {
                    "state" : "success",
                    "responsecode" : "0",
                    "response_date" : getCurrentDateTime(),
                    "result" : bizContentResult
                    };
    
    
                    let resQuery = `
                        UPDATE inv_upstream_input_formatted
                        SET state = $1,
                            responsecode = $2,
                            response_date = $3
                        WHERE uuid = $4;
                        `;
    
                    let resVal = [
                        formatted_res.state,
                        formatted_res.responsecode,
                        formatted_res.response_date,
                        formattedUuid
                    ];
    
                    await pool.query(resQuery, resVal);
    
    
                    let rawResQuery = `
                        UPDATE inv_upstream_input_raw
                        SET rawresponse = $1,
                            response_date = $2
                        WHERE uuid = $3;
                        `;
    
                    let rawResVal = [
                        JSON.stringify(formatted_res),
                        formatted_res.response_date,
                        rawUuid
                    ];
    
                    await pool.query(rawResQuery, rawResVal);
    
                    delete formatted_res.result.allocated
                    delete formatted_res.result.assemblyallocated
                    delete formatted_res.result.assemblyshippingquantity
                    delete formatted_res.result.firstlegallocated
                    delete formatted_res.result.firstlegshippingquantity
                    delete formatted_res.result.manualshippingquantity
                    delete formatted_res.result.orderallocated
                    delete formatted_res.result.purchaseshippingquantity
                    delete formatted_res.result.returnshippingquantity
                    delete formatted_res.result.shippingquantity
                    delete formatted_res.result.total
                    delete formatted_res.result.transferallocated
                    delete formatted_res.result.transfershippingquantity
                    delete formatted_res.result.unavailable
                    delete formatted_res.result.warehousecode


                    return await formatted_res;

    
    
                }else{ // 0 && 1
                    const errorcode = toLowerCaseKeys(bizContentState.errorCode).trim();

                    let responseCode = "1"; // default value

                    if (errorcode.startsWith('SKU_')) {
                    responseCode = "2";
                    } 
                    
                    //create SMF RESPONSE
                    const formatted_res = {
                    "state" : "failure",
                    "responsecode" : responseCode,
                    "response_date" : getCurrentDateTime()
                    };
    
    
                    let resQuery = `
                        UPDATE inv_upstream_input_formatted
                        SET state = $1,
                            responsecode = $2,
                            response_date = $3
                        WHERE uuid = $4;
                        `;
    
                    let resVal = [
                        formatted_res.state,
                        formatted_res.responsecode,
                        formatted_res.response_date,
                        formattedUuid
                    ];
    
                    await pool.query(resQuery, resVal);
    
    
                    let rawResQuery = `
                        UPDATE inv_upstream_input_raw
                        SET rawresponse = $1,
                            response_date = $2
                        WHERE uuid = $3;
                        `;
    
                    let rawResVal = [
                        JSON.stringify(formatted_res),
                        formatted_res.response_date,
                        rawUuid
                    ];
    
                    await pool.query(rawResQuery, rawResVal);
                    return await formatted_res;
    
                }
            }else{
                let baseRes = `
                    UPDATE inv_base_req
                    SET state = $1,
                        errorcode = $2,
                        errormsg = $3,
                        response_date = $4
                    WHERE uuid = $5;
                    `;

                let baseResVal = [
                    response.data.state,
                    response.data.errorCode,
                    response.data.errorMsg,
                    getCurrentDateTime(),
                    baseReqUuid
                ];

                await pool.query(baseRes, baseResVal);

                //create SMF RESPONSE
                const formatted_res = {
                "state" : "failure",
                "responsecode" : "1",
                "response_date" : getCurrentDateTime()
                };

                let resQuery = `
                    UPDATE inv_upstream_input_formatted
                    SET state = $1,
                        responsecode = $2,
                        response_date = $3
                    WHERE uuid = $4;
                    `;

                let resVal = [
                    formatted_res.state,
                    formatted_res.responsecode,
                    formatted_res.response_date,
                    formattedUuid
                ];

                await pool.query(resQuery, resVal);


                let rawResQuery = `
                    UPDATE inv_upstream_input_raw
                    SET rawresponse = $1,
                        response_date = $2
                    WHERE uuid = $3;
                    `;

                let rawResVal = [
                    JSON.stringify(formatted_res),
                    formatted_res.response_date,
                    rawUuid
                ];

                await pool.query(rawResQuery, rawResVal);
                return await formatted_res;
            }

        
        } else { //1 && 1

            let baseRes = `
                UPDATE inv_base_req
                SET state = $1,
                    errorcode = $2,
                    errormsg = $3,
                    response_date = $4
                WHERE uuid = $5;
                `;

            let baseResVal = [
                response.data.state,
                response.data.errorCode,
                response.data.errorMsg,
                getCurrentDateTime(),
                baseReqUuid
            ];

            await pool.query(baseRes, baseResVal);

            //create SMF RESPONSE
            const formatted_res = {
            "state" : "failure",
            "responsecode" : "1",
            "response_date" : getCurrentDateTime()
            };

            let resQuery = `
                UPDATE inv_upstream_input_formatted
                SET state = $1,
                    responsecode = $2,
                    response_date = $3
                WHERE uuid = $4;
                `;

            let resVal = [
                formatted_res.state,
                formatted_res.responsecode,
                formatted_res.response_date,
                formattedUuid
            ];

            await pool.query(resQuery, resVal);


            let rawResQuery = `
                UPDATE inv_upstream_input_raw
                SET rawresponse = $1,
                    response_date = $2
                WHERE uuid = $3;
                `;

            let rawResVal = [
                JSON.stringify(formatted_res),
                formatted_res.response_date,
                rawUuid
            ];

            await pool.query(rawResQuery, rawResVal);
            return await formatted_res;
        }


    }catch (err) {
       // Axios timeout
        if (err.code === 'ECONNABORTED') {
        logger.upstream.error('ERP request timeout');

        const formatted_res = {
            state: "failure",
            responsecode: "1", 
            response_date: getCurrentDateTime()
        };

        // Update upstream formatted table
        const upstreamQue = `
            UPDATE inv_upstream_input_formatted
            SET state = $1,
                responsecode = $2,
                response_date = $3
            WHERE uuid = $4
            RETURNING rawuuid;
        `;

        const upstreamVal = [
            formatted_res.state,
            formatted_res.responsecode,
            formatted_res.response_date,
            formattedUuid
        ];

        const queryUuid = await pool.query(upstreamQue, upstreamVal);
        const rawUuid = queryUuid.rows[0].rawuuid;

        // Update raw response
        const rawRes = `
            UPDATE inv_upstream_input_raw
            SET rawresponse = $1::jsonb,
                response_date = $2
            WHERE uuid = $3;
        `;

        await pool.query(rawRes, [
            JSON.stringify(formatted_res),
            formatted_res.response_date,
            rawUuid
        ]);

        const baseRes = `
        UPDATE inv_base_req
            SET state = $1,
                bizcontent = $2::jsonb,
                response_date = $3
            WHERE uuid = $4;
        `;

        let baseResVal = [
            formatted_res.state,
            formatted_res,
            formatted_res.response_date,
            baseReqUuid
        ];

        await pool.query(baseRes, baseResVal);
        return formatted_res;
        }

        if (err.response) {
        logger.upstream.error(
            'ERP error response:',
            JSON.stringify(err.response.data)
        );
        } else {
        logger.upstream.error('ERP request failed:', err.message);
        }

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
