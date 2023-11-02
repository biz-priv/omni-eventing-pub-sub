const AWS = require("aws-sdk");
const dynamoDB = new AWS.DynamoDB.DocumentClient();
const sns = new AWS.SNS({ apiVersion: "2010-03-31" });
const _ = require("lodash");


module.exports.handler = async (event, context) => {
  try {
    console.info("event:", JSON.stringify(event));
    const processingPromises = event.Records.map(async (record) => {
      const newImage = AWS.DynamoDB.Converter.unmarshall(
        record.dynamodb.NewImage
      );
      if (_.get(newImage, "deliveryStatus", "") === "Pending") {
        const payload = JSON.parse(_.get(newImage, "payload"));
        await processAndDeliverMessage(payload, _.get(newImage, "customerId"));
        await updateMessageStatus(_.get(newImage, "id"), "Delivered");
      }
      return "Success";
    });
    await Promise.all(processingPromises);
  } catch (error) {
    const message = `An error occurred in function ${context.functionName}. Error details: ${error}.`;
    const subject = `Lambda function ${context.functionName} has failed.`;
    await sendSNSNotification(message, subject);
    console.error(error);
  }
};

async function getTopicArn(snsEventType) {
  try {
    const params = {
      Key: {
        Event_Type: snsEventType,
      },
      TableName: process.env.EVENTING_TOPICS_TABLE,
    };
    const response = await dynamoDB.get(params).promise();
    return {
      TopicArn: response.Item.Full_Payload_Topic_Arn,
    };
  } catch (error) {
    console.error(error);
    throw error;
  }
}

async function processAndDeliverMessage(item, customerId) {
  try {
    const { TopicArn } = await getTopicArn("ShipmentAndMilestone");
    console.info("TopicArn:", TopicArn);
    const params = {
      Message: JSON.stringify(item),
      TopicArn,
      MessageAttributes: {
        customer_id: {
          DataType: "String",
          StringValue: customerId.toString(),
        },
      },
    };
    const response = await sns.publish(params).promise();
    console.info("SNS publish:::: ", response);
  } catch (error) {
    console.info("SNSPublishError: ", error);
    throw error;
  }
}

async function updateMessageStatus(id, status) {
  const params = {
    TableName: process.env.SHIPMENT_EVENT_STATUS_TABLE,
    Key: {
      id: id,
    },
    UpdateExpression: "SET #deliveryStatus = :status",
    ExpressionAttributeNames: {
      "#deliveryStatus": "deliveryStatus",
    },
    ExpressionAttributeValues: {
      ":status": status,
    },
  };

  try {
    await dynamoDB.update(params).promise();
    console.info("Updated the status to delivered");
  } catch (error) {
    console.error("Error updating message status:", error);
    throw error;
  }
}

async function sendSNSNotification(message, subject) {
  const snsParams = {
    Message: message,
    Subject: subject,
    TopicArn: process.env.ERROR_SNS_ARN,
  };
  await sns.publish(snsParams).promise();
}
