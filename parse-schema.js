import parseSchema from "mongodb-schema";
import MongoClient from "mongodb";

const dbName = "tourism";
const collectionName = "pois";
const numberOfDocuments = 100000;

function extractField(field) {
  // ignore fields that could not be inferred
  if (!field.types.length > 0 || !field.types[0].bsonType) {
    // console.log(`Could not infer type for: ${JSON.stringify(field)}`);
    return {};
  }
  if (field.types[0].probability <= 0.9) {
    // ignore types with probability not 100%
    // console.warn(
    //   `${field.name} types probably: ${field.types
    //     .map((x) => `${x.name} (${x.probability})`)
    //     .join(", ")}`
    // );
    return {};
  }
  const bsonType = field.types[0].bsonType.toLowerCase();
  if (bsonType === "null") {
    // ignore null type
    return {};
  } else if (bsonType === "document") {
    // parse document
    const properties = field.types[0].fields
      .map(extractField)
      .reduce((obj, x) => ({ ...obj, ...x }), {});
    return {
      [field.name]: {
        bsonType: "object",
        properties: properties,
      },
    };
  } else if (bsonType === "array") {
    // const bsonType = field.types[0].bsonType;
    // if (bsonType == )
    // const items = { bsonType };
    const items = extractField(field.types[0])["Array"];
    return {
      [field.name]: {
        bsonType,
        items,
      },
    };
  } else {
    return {
      [field.name]: {
        bsonType,
      },
    };
  }
}

MongoClient.connect(
  `mongodb://localhost:27017/${dbName}`,
  { useNewUrlParser: true, useUnifiedTopology: true },
  (err, client) => {
    if (err) return console.error(err);

    const db = client.db(dbName);

    parseSchema(
      db.collection(collectionName).find(null, { limit: numberOfDocuments }),
      { semanticTypes: true },
      (err, schema) => {
        if (err) return console.error(err);

        // console.log(JSON.stringify(schema, null, 2));
        const mongoSchema = schema.fields.reduce(
          (obj, x) => ({ ...obj, ...extractField(x) }),
          {}
        );
        console.log(
          JSON.stringify(
            {
              title: collectionName,
              properties: mongoSchema,
            },
            null,
            2
          )
        );
        client.close();
      }
    );
  }
);
