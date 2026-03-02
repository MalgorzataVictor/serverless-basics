import { Handler } from "aws-lambda";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, ScanCommand } from "@aws-sdk/lib-dynamodb";

export const handler: Handler = async () => {
console.log("TABLE_NAME =", process.env.TABLE_NAME);
console.log("REGION =", process.env.REGION);
  try {
    const tableName = process.env.TABLE_NAME;
    const region = process.env.REGION;

    if (!tableName || !region) {
      console.error("TABLE_NAME or REGION env variable not set");
      return {
        statusCode: 500,
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ error: "Missing TABLE_NAME or REGION environment variable" }),
      };
    }

    // Create a DynamoDB Document Client inside the handler
    const ddbDocClient = createDocumentClient(region);

    // Scan the entire Movies table
    const commandOutput = await ddbDocClient.send(
      new ScanCommand({
        TableName: tableName,
      })
    );

    const movies = commandOutput.Items || [];

    return {
      statusCode: 200,
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ data: movies }),
    };
  } catch (error: any) {
    console.error("Error scanning table:", error);
    return {
      statusCode: 500,
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ error: error.message || error }),
    };
  }
};

function createDocumentClient(region: string) {
  const ddbClient = new DynamoDBClient({ region });
  const marshallOptions = {
    convertEmptyValues: true,
    removeUndefinedValues: true,
    convertClassInstanceToMap: true,
  };
  const unmarshallOptions = { wrapNumbers: false };
  const translateConfig = { marshallOptions, unmarshallOptions };
  return DynamoDBDocumentClient.from(ddbClient, translateConfig);
}