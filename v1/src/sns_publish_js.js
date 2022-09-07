const csvtojson = require("csvtojson");
const crypto = require("crypto");
const AWS = require("aws-sdk");
const s3 = new AWS.S3();
const dynamodb = new AWS.DynamoDB({ apiVersion: "2012-08-10" });
const sns = new AWS.SNS({ apiVersion: "2010-03-31" });
const { Client } = require("pg");

module.exports.handler = async (event, context, callback) => {
  let bucket,
    key,
    mergedList,
    changedOldOrders,
    newOrders,
    snsEventType,
    unPublishedList;

  console.log("event:::", event);
  try {
    bucket = event.Records[0].s3.bucket.name;
    key = event.Records[0].s3.object.key;

    const end = "-diff.csv000";
    const eventTopic = key
      .split("/" + process.env.stage + "-")[1]
      .split(end)[0];
    if (!event.hasOwnProperty("existing")) {
      let jsonFromCsv = await getS3Object(bucket, key);

      if (eventTopic.includes("shipment-info")) {
        let eventData = await getEventData(jsonFromCsv, "ShipmentUpdates");
        changedOldOrders = eventData.changedOldOrders;
        newOrders = eventData.newOrders;

        if (changedOldOrders == null && newOrders == null) {
          event.existing = true;
          event.input = [];
          console.log("No Valid Shipment Info");
          return event;
        }
        snsEventType = "ShipmentUpdates";
      } else if (eventTopic.includes("customer-invoices")) {
        let eventData = await getEventData(jsonFromCsv, "CustomerInvoices");
        changedOldOrders = eventData.changedOldOrders;
        newOrders = eventData.newOrders;
        if (changedOldOrders == null && newOrders == null) {
          event.existing = true;
          event.input = [];
          console.log("No Valid Customer Invoices Info");
          return event;
        }
        snsEventType = "CustomerInvoices";
      } else if (eventTopic.includes("shipment-milestone")) {
        console.log("event:::", event);
        let eventData = await getEventsMilestones(jsonFromCsv);
        changedOldOrders = eventData.changedOldOrders;
        newOrders = eventData.newOrders;
        if (changedOldOrders == null && newOrders == null) {
          event.existing = true;
          event.input = [];
          console.log("No Valid Shipment Milestone Info");
          return event;
        }
        snsEventType = "Milestone";
      }

      //**************
      let changedOldOrdersWithSecret = await includeSharedSecret(
        changedOldOrders,
        snsEventType
      );
      let newOrdersSecret = await includeSharedSecret(newOrders, snsEventType);

      mergedList = newOrdersSecret.concat(changedOldOrdersWithSecret);
    } else {
      mergedList = event.input;
      if (eventTopic.includes("shipment-info")) {
        snsEventType = "ShipmentUpdates";
      } else if (eventTopic.includes("customer-invoices")) {
        snsEventType = "CustomerInvoices";
      } else if (eventTopic.includes("shipment-milestone")) {
        snsEventType = "Milestone";
      }
    }
    if (mergedList.length == 0) {
      event.end = "true";
    } else {
      await Promise.all(
        mergedList.slice(0, 20).map(async (item, i) => {
          await snsPublish(item, snsEventType);
          mergedList[i].published = true;
        })
      );
      unPublishedList = mergedList.filter((e) => e.published == false);
      if (unPublishedList.length == 0) {
        event.end = "true";
      } else {
        event.end = "false";
      }
    }
    event.existing = true;
    event.input = unPublishedList;
  } catch (error) {
    console.log("handlerError", error);
  }
  console.log("event::", JSON.stringify(event));
  return event;
};

async function getEventData(data, type) {
  try {
    let emptyBillNumber = "Returning No valid shipment information";
    let jsonError = "ShipmentInfoJSONError";
    if (type == "CustomerInvoices") {
      emptyBillNumber = "Returning No valid Customer Invoices details";
      jsonError = "EventsJsonInvoicesError";
    }
    let filteredData = data.filter((element) => element.bill_to_nbr.length > 0);
    filteredData = replaceNull(filteredData);
    //unique bill no
    let distinctBilNumbers = getDistinctBilNumbers(filteredData);
    if (distinctBilNumbers.length == 0) {
      return { changedOldOrders: null, newOrders: null };
    }

    //get customer ids
    let custIdData = await getCustId(distinctBilNumbers);
    let { oldData, newData } = getOldAndNewData(filteredData);
    let distinctOldFileNumbers = [
      ...new Set(oldData.map((item) => item.file_nbr)),
    ];

    let newDataWithCusId = mergeFilteredDataWithCustomerId(newData, custIdData);
    let newOrders = newDataWithCusId.map((item) => ({
      ...item,
      SNS_FLAG: "FULL",
      published: false,
    }));

    let oldDataWithCusId = mergeFilteredDataWithCustomerId(oldData, custIdData);
    let changedOldOrders = getDifferenceData(
      distinctOldFileNumbers,
      newDataWithCusId,
      oldDataWithCusId,
      "single"
    );
    return { changedOldOrders: changedOldOrders, newOrders: newOrders };
  } catch (error) {
    console.log(jsonError, error);
  }
}

async function getEventsMilestones(data) {
  try {
    let filteredData = data.filter((element) => {
      return element.bill_to_nbr.length > 0 && element.source_system != "EE";
    });
    filteredData = replaceNull(filteredData);
    let distinctOrderStatus = [
      ...new Set(filteredData.map((item) => item.order_status)),
    ];
    let x12CodesOfDistinctOrderStatus = await getX12Codes(distinctOrderStatus);
    let filteredDataWithX12 = await mergeFilteredDataWithX12Codes(
      filteredData,
      x12CodesOfDistinctOrderStatus
    );

    //unique bill no
    let distinctBilNumbers = getDistinctBilNumbers(filteredDataWithX12);

    if (distinctBilNumbers.length == 0) {
      console.log("Returning No valid Shipment Milestone Details");
      return { changedOldOrders: null, newOrders: null };
    }
    let custIdData = await getCustId(distinctBilNumbers);
    let filteredDataWithCustId = mergeFilteredDataWithCustomerId(
      filteredDataWithX12,
      custIdData,
      false
    );
    let { oldData, newData } = getOldAndNewData(filteredDataWithCustId);
    let distinctOldFileNumbers = [
      ...new Set(oldData.map((item) => item.file_nbr)),
    ];

    let changedOldOrders = getDifferenceData(
      distinctOldFileNumbers,
      newData,
      oldData,
      "multi"
    );
    let newOrders = newData.map((item) => ({
      ...item,
      SNS_FLAG: "FULL",
      published: false,
    }));
    return { changedOldOrders: changedOldOrders, newOrders: newOrders };
  } catch (error) {
    console.log("getEventsMilestonesError", error);
  }
}

function getDistinctBilNumbers(data) {
  return [...new Set(data.map((item) => item.bill_to_nbr))];
}
function replaceNull(data) {
  return JSON.parse(JSON.stringify(data).replace(/\:null/gi, ':"NA"'));
}

function getOldAndNewData(data) {
  let oldData = data.filter((el) => el.record_type == "OLD");
  let newData = data.filter((el) => el.record_type == "NEW");
  return { oldData, newData };
}

function connectPgDb() {
  return {
    database: process.env.DBNAME,
    host: process.env.HOST,
    port: process.env.PORT,
    user: process.env.USER,
    password: process.env.PASS,
  };
}

function getS3Object(bucket, key) {
  return new Promise(async (resolve, reject) => {
    try {
      const params = {
        Bucket: bucket,
        Key: key,
      };
      const csvFile = s3.getObject(params).createReadStream();
      const jsonFromCsv = await csvtojson().fromStream(csvFile);
      resolve(jsonFromCsv);
    } catch (error) {
      console.log("S3GetObjectError: ", error);
      reject(error);
    }
  });
}

async function getX12Codes(distinctOrderStatus) {
  return new Promise(async (resolve, reject) => {
    try {
      let params = [];
      for (let i = 1; i <= distinctOrderStatus.length; i++) {
        params.push("$" + i);
      }
      const client = new Client(connectPgDb());
      client.connect();
      client
        .query(
          "select x12_cd, x12_event_desc, omni_cd from public.x12_cross_ref where omni_cd in (" +
            params.join(",") +
            ")",
          distinctOrderStatus
        )
        .then((result) => {
          client.end();
          resolve(result.rows);
        })
        .catch((e) => {
          client.end();
          console.error(e.stack);
          reject(e.stack);
        });
    } catch (error) {
      console.log("GetX12CodesError: ", error);
      reject(error);
    }
  });
}

function mergeFilteredDataWithX12Codes(
  filteredData,
  x12CodesOfDistinctOrderStatus
) {
  try {
    return filteredData.map((t1) => {
      let obj = {
        ...t1,
        ...x12CodesOfDistinctOrderStatus.find(
          (t2) => t2.omni_cd === t1.order_status
        ),
      };
      if (obj.hasOwnProperty("omni_cd")) delete obj.omni_cd;
      return obj;
    });
  } catch (error) {
    console.log("RawDataX12CodesMergeError: ", error);
  }
}

function getCustId(distinctBilNumbers) {
  return new Promise(async (resolve, reject) => {
    try {
      let params = [];
      for (let i = 1; i <= distinctBilNumbers.length; i++) {
        params.push("$" + i);
      }

      const client = new Client(connectPgDb());
      client.connect();
      client
        .query(
          "select id, cust_nbr, source_system from public.api_token where cust_nbr in (" +
            params.join(",") +
            ")",
          distinctBilNumbers
        )
        .then((result) => {
          const formattedData = result.rows.map(
            ({
              id: customer_id,
              cust_nbr: bill_to_nbr,
              source_system: source_system,
              ...rest
            }) => ({
              customer_id,
              bill_to_nbr,
              source_system,
              ...rest,
            })
          );
          client.end();
          resolve(formattedData);
        })
        .catch((e) => {
          client.end();
          console.error(e.stack);
          reject();
        });
    } catch (error) {
      console.log("getCustId: ", error);
      reject();
    }
  });
}

function mergeFilteredDataWithCustomerId(
  filteredData,
  custIdData,
  unique = true
) {
  try {
    let filteredDataWithCid = filteredData.map((t1) => {
      return {
        ...t1,
        ...custIdData.find(
          (t2) =>
            t2.bill_to_nbr === t1.bill_to_nbr &&
            t2.source_system === t1.source_system
        ),
      };
    });
    filteredDataWithCid = filteredDataWithCid.filter(
      (e) => e.customer_id && e.customer_id != null
    );
    if (unique) {
      const filteredDataWithCidUnique = [
        ...new Map(filteredDataWithCid.map((item) => [item.id, item])).values(),
      ];
      return filteredDataWithCidUnique;
    } else {
      return filteredDataWithCid;
    }
  } catch (error) {
    console.log("RawDataCustomerIDMergeError: ", error);
  }
}

function includeSharedSecret(payload, snsEventType) {
  return new Promise(async (resolve, reject) => {
    try {
      Promise.all(
        payload.map(async (item) => {
          if (!item.customer_id || item.customer_id.length == 0) return null;

          let sharedSecret = await getSharedSecret(
            item.customer_id,
            snsEventType
          );
          if (sharedSecret !== null) {
            item.Signature = crypto
              .createHmac("sha256", sharedSecret)
              .update(JSON.stringify(item))
              .digest("hex");
            return item;
          } else {
            return null;
          }
        })
      ).then((data) => {
        resolve(data.filter((item) => item != null));
      });
    } catch (error) {
      console.log("PublishMessageError: ", error);
      resolve(error);
    }
  });
}

function getSharedSecret(cust_id, snsEventType) {
  return new Promise(async (resolve, reject) => {
    try {
      const params = {
        Key: {
          Customer_Id: { S: cust_id.toString() },
          Event_Type: { S: snsEventType },
        },
        TableName: process.env.CUSTOMER_PREFERENCE_TABLE,
      };
      const response = await dynamodb.getItem(params).promise();
      if (response.Item && response.Item.Shared_Secret) {
        resolve(response.Item.Shared_Secret.S);
      } else {
        resolve(null);
      }
    } catch (error) {
      console.error(error);
      reject(error);
    }
  });
}

function snsPublish(item, snsEventType) {
  return new Promise(async (resolve, reject) => {
    try {
      const { changeTopicArn, newTopicArn } = await getTopicArn(snsEventType);
      let TopicArn;
      if (item.SNS_FLAG == "DIFF") TopicArn = changeTopicArn;
      else if (item.SNS_FLAG == "FULL") TopicArn = newTopicArn;

      const params = {
        Message: JSON.stringify(item),
        TopicArn,
        MessageAttributes: {
          customer_id: {
            DataType: "String",
            StringValue: item.customer_id.toString(),
          },
        },
      };
      //SNS service
      const response = await sns.publish(params).promise();
      console.log("SNS publish:::: ", response);
      resolve(1);
    } catch (error) {
      console.log("SNSPublishError: ", error);
      reject(error);
    }
  });
}

function getTopicArn(snsEventType) {
  return new Promise(async (resolve, reject) => {
    try {
      const params = {
        Key: {
          Event_Type: { S: snsEventType },
        },
        TableName: process.env.EVENTING_TOPICS_TABLE,
      };
      const response = await dynamodb.getItem(params).promise();
      resolve({
        changeTopicArn: response.Item.Event_Payload_Topic_Arn.S,
        newTopicArn: response.Item.Full_Payload_Topic_Arn.S,
      });
    } catch (error) {
      console.error(error);
      reject(error);
    }
  });
}

function getDifferenceData(distinctOldFileNumbers, newData, oldData, type) {
  let result = [];
  distinctOldFileNumbers.map((item) => {
    //get all new data by file_nbr
    let matchedNewData = newData.filter((e) => e.file_nbr == item);

    //sortBy date
    matchedNewData = matchedNewData.sort((a, b) =>
      new Date(a.event_date).getTime() > new Date(b.event_date).getTime()
        ? 1
        : new Date(b.event_date).getTime() > new Date(a.event_date).getTime()
        ? -1
        : 0
    );

    if (matchedNewData.length > 0) {
      // get old data by file_nbr
      let filterdOldData = oldData.filter((e) => e.file_nbr == item);
      if (filterdOldData.length > 0) {
        if (filterdOldData.length > 1) {
          filterdOldData = filterdOldData.sort((a, b) =>
            new Date(a.event_date).getTime() > new Date(b.event_date).getTime()
              ? 1
              : new Date(b.event_date).getTime() >
                new Date(a.event_date).getTime()
              ? -1
              : 0
          );
        }

        filterdOldData = filterdOldData[filterdOldData.length - 1];

        let diffArray = [];
        matchedNewData.map((e, i) => {
          let oldCompereableData =
            i == 0 ? filterdOldData : matchedNewData[i - 1];

          let diff = Object.keys(e).reduce((diff, key) => {
            if (oldCompereableData[key] === e[key]) return diff;
            return {
              ...diff,
              [key]: { old: oldCompereableData[key], new: e[key] },
            };
          }, {});

          if (type == "multi") diffArray.push(diff);
          else diffArray = diff;
        });
        let matchObj = matchedNewData[matchedNewData.length - 1];
        result.push({
          id: matchObj.id,
          file_nbr: matchObj.file_nbr,
          customer_id: matchObj.customer_id,
          changes: diffArray,
          SNS_FLAG: "DIFF",
          published: false,
        });
      }
    }
  });
  return result;
}
