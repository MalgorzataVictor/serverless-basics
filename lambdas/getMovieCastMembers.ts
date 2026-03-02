import { Handler } from "aws-lambda";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import {
  DynamoDBDocumentClient,
  QueryCommand,
  QueryCommandInput,
  GetCommand,
} from "@aws-sdk/lib-dynamodb";

import { MovieCastResponse, MovieCast } from "../shared/types";

const ddbDocClient = createDocumentClient();

type MovieMetadata = {
  title: string;
  genre_ids: number[];
  overview: string;
};

export const handler: Handler = async (event, context) => {
  try {
    console.log("Event: ", JSON.stringify(event));
    const queryParams = event?.queryStringParameters;
    const includeMovie = queryParams?.movie === 'true';

    if (!queryParams) {
      return {
        statusCode: 500,
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ message: "Missing query parameters" }),
      };
    }

    if (!queryParams.movieId) {
      return {
        statusCode: 500,
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ message: "Missing movie Id parameter" }),
      };
    }

    const movieId = parseInt(queryParams.movieId);

    // Step 1: Query Cast Table
    let commandInput: QueryCommandInput = {
      TableName: process.env.CAST_TABLE_NAME!,
    };

    if ("roleName" in queryParams) {
      commandInput = {
        ...commandInput,
        IndexName: "roleIx",
        KeyConditionExpression: "movieId = :m and begins_with(roleName, :r)",
        ExpressionAttributeValues: {
          ":m": movieId,
          ":r": queryParams.roleName,
        },
      };
    } else if ("actorName" in queryParams) {
      commandInput = {
        ...commandInput,
        KeyConditionExpression: "movieId = :m and begins_with(actorName, :a)",
        ExpressionAttributeValues: {
          ":m": movieId,
          ":a": queryParams.actorName,
        },
      };
    } else {
      commandInput = {
        ...commandInput,
        KeyConditionExpression: "movieId = :m",
        ExpressionAttributeValues: { ":m": movieId },
      };
    }

    const commandOutput = await ddbDocClient.send(new QueryCommand(commandInput));
    const castItems: MovieCast[] = (commandOutput.Items as MovieCast[]) || [];

    // Step 2: Optionally fetch movie metadata
    let movieData: MovieMetadata | undefined;
    if (includeMovie) {
      const movieOutput = await ddbDocClient.send(
        new GetCommand({
          TableName: process.env.MOVIES_TABLE_NAME!,
          Key: { id: movieId },
          ProjectionExpression: "title, genre_ids, overview",
        })
      );
      movieData = movieOutput.Item as MovieMetadata;
    }

    // Step 3: Prepare response
    const response: MovieCastResponse = {
      cast: castItems,
      movie: includeMovie ? movieData : undefined,
    };

    return {
      statusCode: 200,
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ data: response }),
    };
  } catch (error: any) {
    console.log(JSON.stringify(error));
    return {
      statusCode: 500,
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ error }),
    };
  }
};

function createDocumentClient() {
  const ddbClient = new DynamoDBClient({ region: process.env.REGION });
  const marshallOptions = {
    convertEmptyValues: true,
    removeUndefinedValues: true,
    convertClassInstanceToMap: true,
  };
  const unmarshallOptions = {
    wrapNumbers: false,
  };
  const translateConfig = { marshallOptions, unmarshallOptions };
  return DynamoDBDocumentClient.from(ddbClient, translateConfig);
}