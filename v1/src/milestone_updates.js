const AWS = require("aws-sdk");
const dynamoDB = new AWS.DynamoDB.DocumentClient();
const sns = new AWS.SNS({ apiVersion: "2010-03-31" });
const moment = require("moment-timezone");
const _ = require("lodash");
const { v4: uuidv4 } = require("uuid");

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

module.exports.handler = async (event, context) => {
  console.info("event:", JSON.stringify(event));
  try {
    await Promise.all(
      event.Records.map(async (record) => {
        const newImage = _.get(record, "dynamodb.NewImage");

        // Check if ProcessState is equal to 'Not Processed'
        if (_.get(newImage, "ProcessState.S", "") === "Not Processed") {
          try {
            // Update a column in the same table to set ProcessState as 'Pending'
            await updateProcessState(newImage, "Pending");
            const payload = await processDynamoDBRecord(newImage);
            console.log("payload:",payload);
            const customerId = await GetCustomer(_.get(payload, "trackingNo"));
            await saveToDynamoDB(payload, customerId, "Pending"); // Default status as Pending
            // Update a column in the same table to set ProcessState as 'Processed'
            await updateProcessState(newImage, "Processed");
          } catch (error) {
            console.error(`Error processing record: ${error.message}`);
            // Save the error message to SHIPMENT_EVENT_STATUS_TABLE with status "Error"
            await saveToDynamoDB(
              {
                id: uuidv4(),
                trackingNo: "No trackingNo",
                InsertedTimeStamp: moment
                  .tz("America/Chicago")
                  .format("YYYY:MM:DD HH:mm:ss")
                  .toString(),
                deliveryStatus: "Error",
                errorMessage: error,
              },
              "",
              "Error"
            );
          }
        }
      })
    );
    console.info("The record is processed");
  } catch (error) {
    const errorMessage = `An error occurred in function ${context.functionName}. Error details: ${error}.`;
    console.error(errorMessage);
    try {
      await publishToSNS(errorMessage, context.functionName);
    } catch (snsError) {
      console.error("Error publishing to SNS:", snsError);
    }
  }
};

async function updateProcessState(newImage, processState) {
  const orderNo = _.get(newImage, "FK_OrderNo.S", "");
  const orderStatusId = _.get(newImage, "FK_OrderStatusId.S", "");

  if (!orderNo || !orderStatusId) {
    throw new Error("Missing orderNo or orderStatusId in the newImage.");
  }

  const params = {
    TableName: process.env.SHIPMENT_MILESTONE_TABLE,
    Key: {
      FK_OrderNo: orderNo,
      FK_OrderStatusId: orderStatusId,
    },
    UpdateExpression: "SET ProcessState = :processState",
    ExpressionAttributeValues: {
      ":processState": processState,
    },
  };

  try {
    await dynamoDB.update(params).promise();
    console.info(`Updated the ProcessState to '${processState}'`);
  } catch (error) {
    console.error("Error updating ProcessState:", error.message);
    throw error;
  }
}

async function publishToSNS(message, subject) {
  const params = {
    Message: message,
    Subject: `Lambda function ${subject} has failed.`,
    TopicArn: process.env.ERROR_SNS_ARN,
  };
  try {
    await sns.publish(params).promise();
  } catch (error) {
    console.error(error);
  }
}

async function processDynamoDBRecord(dynamodbRecord) {
  const {
    FK_OrderNo: { S: OrderNo },
    FK_OrderStatusId: { S: OrderStatusId },
    EventDateTime: { S: EventDateTime },
    UUid: { S: id },
  } = dynamodbRecord;

  const headerDetails = await queryHeaderDetails(OrderNo);
  const { ETADateTime, Housebill } = headerDetails;

  const shipperDetails = await queryShipperDetails(OrderNo);
  const consigneeDetails = await queryConsigneeDetails(OrderNo);

  if (
    !OrderNo ||
    !OrderStatusId ||
    !EventDateTime ||
    !Housebill ||
    !shipperDetails ||
    !consigneeDetails
  ) {
    console.error("One or more mandatory fields are missing in the payload");
  }


  const stopsequence = statusMapping[OrderStatusId] ? statusMapping[OrderStatusId].stopSequence : 2;
  const statusInfo = statusMapping[OrderStatusId];

  const payload = {
    id: id,
    trackingNo: Housebill,
    carrier: _.get(shipperDetails, "ShipName", ""),
    statusCode: OrderStatusId,
    lastUpdateDate: EventDateTime,
    estimatedDeliveryDate:
      ETADateTime === "1900-01-01 00:00:00.000" ? "NA" : ETADateTime,
    identifier: "NA",
    statusDescription: _.get(statusInfo, "description"),
    retailerMoniker: "dell",
    originCity: _.get(shipperDetails, "ShipCity", ""),
    originState: _.get(shipperDetails, "FK_ShipState", ""),
    originZip: _.get(shipperDetails, "ShipZip", ""),
    originCountryCode: _.get(shipperDetails, "FK_ShipCountry", ""),
    destCity: _.get(consigneeDetails, "ConCity", ""),
    destState: _.get(consigneeDetails, "FK_ConState", ""),
    destZip: _.get(consigneeDetails, "ConZip", ""),
    destCountryCode: _.get(consigneeDetails, "FK_ConCountry", ""),
  };
  
  if (stopsequence === 1) {
    payload.eventCity = _.get(shipperDetails, "ShipCity", "Unknown");
    payload.eventState = _.get(shipperDetails, "FK_ShipState", "Unknown");
    payload.eventZip = _.get(shipperDetails, "ShipZip", "Unknown");
    payload.eventCountryCode = _.get(shipperDetails, "FK_ShipCountry", "Unknown");
  } else if (stopsequence === 2) {
    payload.eventCity = _.get(consigneeDetails, "ConCity", "Unknown");
    payload.eventState = _.get(consigneeDetails, "FK_ConState", "Unknown");
    payload.eventZip = _.get(consigneeDetails, "ConZip", "Unknown");
    payload.eventCountryCode = _.get(consigneeDetails, "FK_ConCountry", "Unknown");
  }
  

  return payload;
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
    if (_.get(result, "Items", []).length > 0) {
      return result.Items[0];
    }
  } catch (error) {
    console.error("Error querying shipper details:", error.message);
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
    if (_.get(result, "Items", []).length > 0) {
      return result.Items[0];
    }
  } catch (error) {
    console.error("Error querying consignee details:", error.message);
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
    if (_.get(result, "Items", []).length > 0) {
      return result.Items[0];
    }
  } catch (error) {
    console.error("Error querying header details:", error.message);
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
    if (_.get(data, "Items", []).length > 0) {
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
  }
}
