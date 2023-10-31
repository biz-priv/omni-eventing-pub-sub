const AWS = require("aws-sdk");
const dynamoDB = new AWS.DynamoDB.DocumentClient();
const sns = new AWS.SNS({ apiVersion: "2010-03-31" });

module.exports.handler = async (event, context) => {
  try {
    console.info("event:", JSON.stringify(event));
    // Use map to create an array of promises for processing each record
    const processingPromises = event.Records.map(async (record) => {
      if (record.eventName === "INSERT") {
        // Extract the new record data
        const newImage = AWS.DynamoDB.Converter.unmarshall(
          record.dynamodb.NewImage
        );
        // Parse the stringified payload into a JSON object
        const payload = JSON.parse(newImage.payload);
        // Deliver the message
        await snsPublish(payload, "ShipmentAndMilestone", newImage.customerId);
        // Update the message status to "Delivered" in DynamoDB
        await updateDeliveryStatus(newImage.id, "Delivered");
      }
      return "Success";
    });
    await Promise.all(processingPromises);
  } catch (error) {
    // Send a notification to the SNS topic
    const message = `An error occurred in function ${context.functionName}. Error details: ${error}.`;
    const subject = `Lambda function ${context.functionName} has failed.`;
    const snsParams = {
      Message: message,
      Subject: subject,
      TopicArn: process.env.ERROR_SNS_ARN,
    };
    await sns.publish(snsParams).promise();
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
      newTopicArn: response.Item.Full_Payload_Topic_Arn,
    };
  } catch (error) {
    console.error(error);
    throw error;
  }
}

async function snsPublish(item, snsEventType, customerId) {
  try {
    const { newTopicArn } = await getTopicArn(snsEventType);
    let TopicArn;
    TopicArn = newTopicArn;
    console.info(newTopicArn);
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
    //SNS service
    const response = await sns.publish(params).promise();
    console.info("SNS publish:::: ", response);
  } catch (error) {
    console.info("SNSPublishError: ", error);
    throw error;
  }
}

async function updateDeliveryStatus(id, status) {
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
    console.info("updated the status to delivered");
  } catch (error) {
    console.error("Error updating message status:", error);
    throw error;
  }
}