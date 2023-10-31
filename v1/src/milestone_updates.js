const AWS = require("aws-sdk");
const { v4: uuidv4 } = require("uuid");
const dynamoDB = new AWS.DynamoDB.DocumentClient();
const sns = new AWS.SNS({ apiVersion: "2010-03-31" });
const moment = require("moment-timezone");
const _ = require("lodash")

module.exports.handler = async (event, context) => {
  console.info("event:", JSON.stringify(event));
  try {
    const processingPromises = event.Records.map(async (record) => {
      const dynamodbRecord = record.dynamodb.NewImage;
      const payload = await processDynamoDBRecord(dynamodbRecord);
      console.info("payload:", JSON.stringify(payload));
      const data = replaceNull(payload);
      console.info("housebill", data.trackingNo);
      const customerId = await GetCustomer(data.trackingNo);
      console.info("customerId:", customerId);
      const deliveryStatus = "NO"; // Default status
      await saveToDynamoDB(data, customerId, deliveryStatus);
      return data;
    });

    // Use Promise.all to await all processing promises
    const results = await Promise.all(processingPromises);

    // Filter out any null results (skipped records)
    const successfulResults = results.filter((result) => result !== null);

    console.info("Successfully processed records:", successfulResults);
  } catch (error) {
    // Send a notification to the SNS topic
    const errorMessage = `An error occurred in function ${context.functionName}. Error details: ${error}.`;

    try {
      await publishToSNS(errorMessage, context.functionName);
    } catch (snsError) {
      console.error("Error publishing to SNS:", snsError);
    }
    console.error(error);
  }
};

async function publishToSNS(message, subject) {
  const params = {
    Message: message,
    Subject: `Lambda function ${subject} has failed.`,
    TopicArn: process.env.ERROR_SNS_ARN,
  };

  await sns.publish(params).promise();
}

async function processDynamoDBRecord(dynamodbRecord) {
  try {
    const Id = uuidv4();
    let OrderNo, OrderStatusId, EventDateTime;
    let shipperDetails, consigneeDetails;
    let edd, housebill;

    ({
      FK_OrderNo: { S: OrderNo },
      FK_OrderStatusId: { S: OrderStatusId },
      EventDateTime: { S: EventDateTime },
    } = dynamodbRecord);

    // Query header details to get additional data
    const headerDetails = await queryHeaderDetails(OrderNo);
    edd = headerDetails.ETADateTime;
    housebill = headerDetails.Housebill;

    // Query shipper and consignee details
    shipperDetails = await queryShipperDetails(OrderNo);
    consigneeDetails = await queryConsigneeDetails(OrderNo);

    const data = mapStatusDescription(OrderStatusId);
    const stopsequence = data.statusDescription.stopSequence;
    const result = mapStatusDescription(
      OrderStatusId,
      stopsequence,
      shipperDetails,
      consigneeDetails
    );

    const statusdescription = result.statusDescription.description;
    const eventcity = result.eventDetails.eventCity;
    const eventstate = result.eventDetails.eventState;
    const eventzip = result.eventDetails.eventZip;
    const eventcountry = result.eventDetails.eventCountryCode;
    console.info("stopsequence:", stopsequence);

    console.info("statusDescription:", statusdescription);
    if (edd == "1900-01-01 00:00:00.000") {
      edd = null;
    }
    const payload = {
      id: Id,
      trackingNo: housebill,
      carrier: shipperDetails.ShipName,
      statusCode: OrderStatusId,
      lastUpdateDate: EventDateTime,
      estimatedDeliveryDate: edd,
      identifier: "NA", // not required
      eventCity: eventcity,
      eventState: eventstate,
      eventZip: eventzip,
      eventCountryCode: eventcountry,
      retailerMoniker: "dell", // default value
      statusDescription: statusdescription,
      originCity: shipperDetails.ShipCity,
      originState: shipperDetails.FK_ShipState,
      originZip: shipperDetails.ShipZip,
      originCountryCode: shipperDetails.FK_ShipCountry,
      destCity: consigneeDetails.ConCity,
      destState: consigneeDetails.FK_ConState,
      destZip: consigneeDetails.ConZip,
      destCountryCode: consigneeDetails.FK_ConCountry,
    };

    return payload;
  } catch (error) {
    console.error(`Error processing DynamoDB record: ${error.message}`);
    throw error;
  }
}

// Define the mapping of FK_OrderStatusId to statusDescription
const statusMapping = {
  APU: { description: "PICK UP ATTEMPT", stopSequence: 1 },
  SER: { description: "SHIPMENT EN ROUTE", stopSequence: 1 },
  COB: { description: "INTRANSIT", stopSequence: 1 },
  AAG: { description: "ARRIVED AT DESTINATION GATEWAY", stopSequence: 2 },
  REF: { description: "SHIPMENT REFUSED", stopSequence: 1 },
  APL: { description: "ONSITE", stopSequence: 1 },
  WEB: { description: "NEW WEB SHIPMENT", stopSequence: 1 },
  AAO: { description: "ARRIVED AT OMNI DESTINATION", stopSequence: 2 },
  AAD: { description: "ARRIVED AT DESTINATION", stopSequence: 2 },
  SOS: { description: "EMERGENCY WEATHER DELAY", stopSequence: 1 },
  SDE: { description: "SHIPMENT DELAYED", stopSequence: 1 },
  DGW: { description: "SHIPMENT DEPARTED GATEWAY", stopSequence: 1 },
  TTC: { description: "LOADED", stopSequence: 1 },
  NEW: { description: "NEW SHIPMENT", stopSequence: 1 },
  SRS: { description: "SHIPMENT RETURNED TO SHIPPER", stopSequence: 1 },
  TPC: { description: "TRANSFER TO PARTNER CARRIER", stopSequence: 1 },
  PUP: { description: "PICKED UP", stopSequence: 1 },
  OFD: { description: "OUT FOR DELIVERY", stopSequence: 2 },
  CAN: { description: "CANCELLED", stopSequence: 1 },
  OSD: { description: "SHIPMENT DAMAGED", stopSequence: 1 },
  RCS: { description: "RECONSIGNED", stopSequence: 1 },
  ADL: { description: "DELIVERY ATTEMPTED", stopSequence: 2 },
  LOD: { description: "LOADED", stopSequence: 1 },
  DEL: { description: "DELIVERED", stopSequence: 2 },
  ED: { description: "ESTIMATED DELIVERY", stopSequence: 2 },
};

function mapStatusDescription(
  FK_OrderStatusId,
  stopsequence,
  shipperDetails,
  consigneeDetails
) {
  // Default event details
  let eventDetails = {
    eventCity: "Unknown",
    eventState: "Unknown",
    eventZip: "Unknown",
    eventCountryCode: "Unknown",
  };

  if (stopsequence === 1) {
    // If stop sequence is 1, map to shipper details
    eventDetails = {
      eventCity: _.get(shipperDetails, "ShipCity",""),
      eventState: _.get(shipperDetails,"FK_ShipState",""),
      eventZip: _.get(shipperDetails,"ShipZip",""),
      eventCountryCode: _.get(shipperDetails,"FK_ShipCountry",""),
    };
  } else if (stopsequence === 2) {
    // If stop sequence is 2, map to consignee details
    eventDetails = {
      eventCity: _.get(consigneeDetails,"ConCity",""),
      eventState: _.get(consigneeDetails,"FK_ConState",""),
      eventZip: _.get(consigneeDetails,"ConZip",""),
      eventCountryCode: _.get(consigneeDetails,"FK_ConCountry",""),
    };
  }

  return {
    statusDescription: statusMapping[FK_OrderStatusId] || "Unknown",
    eventDetails: eventDetails,
  };
}

async function queryShipperDetails(OrderNo) {
  const params = {
    TableName: process.env.SHIPPER_TABLE,
    KeyConditionExpression: `FK_ShipOrderNo = :orderNo`,
    ExpressionAttributeValues: {
      ":orderNo": OrderNo,
    },
  };
  try {
    const result = await dynamoDB.query(params).promise();
    return result.Items[0] || {};
  } catch (error) {
    console.error("Error querying shipper details:", error.message);
    throw error;
  }
}

async function queryConsigneeDetails(OrderNo) {
  const params = {
    TableName: process.env.CONSIGNEE_TABLE,
    KeyConditionExpression: `FK_ConOrderNo = :orderNo`,
    ExpressionAttributeValues: {
      ":orderNo": OrderNo,
    },
  };
  try {
    const result = await dynamoDB.query(params).promise();
    return result.Items[0] || {};
  } catch (error) {
    console.error("Error querying consignee details:", error.message);
    throw error;
  }
}

async function queryHeaderDetails(OrderNo) {
  const params = {
    TableName: process.env.SHIPMENT_HEADER_TABLE,
    KeyConditionExpression: `PK_OrderNo = :orderNo`,
    ExpressionAttributeValues: {
      ":orderNo": OrderNo,
    },
  };
  try {
    const result = await dynamoDB.query(params).promise();
    return result.Items[0] || {};
  } catch (error) {
    console.error("Error querying header details:", error.message);
    throw error;
  }
}

async function GetCustomer(housebill) {
  const params = {
    TableName: process.env.CUSTOMER_ENTITLEMENT_TABLE,
    IndexName: process.env.ENTITLEMENT_HOUSEBILL_INDEX,
    KeyConditionExpression: "HouseBillNumber = :Housebill",
    ExpressionAttributeValues: {
      ":Housebill": housebill,
    },
  };

  try {
    const data = await dynamoDB.query(params).promise();
    if (data.Items && data.Items.length > 0) {
      return data.Items[0].CustomerID;
    } else {
      throw new Error(
        `No CustomerID found for this ${housebill} in entitlements table`
      );
    }
  } catch (error) {
    console.error("Validation error:", error);
    throw error;
  }
}

function replaceNull(data) {
  return JSON.parse(
    JSON.stringify(data, (key, value) => {
      if (value === null || value === "") {
        return "NA";
      }
      return value;
    })
  );
}

async function saveToDynamoDB(payload, customerId, deliveryStatus) {
  const params = {
    TableName: process.env.SHIPMENT_EVENT_STATUS_TABLE,
    Item: {
      id: payload.id,
      trackingNo: payload.trackingNo,
      customerId: customerId,
      InsertedTimeStamp: moment
        .tz("America/Chicago")
        .format("YYYY:MM:DD HH:mm:ss")
        .toString(),
      payload: JSON.stringify(payload),
      deliveryStatus: deliveryStatus,
    },
  };

  try {
    await dynamoDB.put(params).promise();
  } catch (error) {
    console.error("Error saving to DynamoDB:", error);
    throw error;
  }
}