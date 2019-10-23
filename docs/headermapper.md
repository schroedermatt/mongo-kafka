# Header Mapper

The MongoDB Sink connector can route data to a namespace (database/collection) by inspecting the 
incoming record headers and building the target namespace dynamically.

The headers that the connector will inspect are `MONGO_DB` and `MONGO_COLL`. 
These headers are optional, meaning that if `MONGO_DB` is not present, the connector falls 
back to the configured `database`. Similarly, if the `MONGO_COLL` header is not present,
the connector will default to the configured `collection`.

Being that this is not the default mapper used, you will have to explicitly configure this functionality.

## Configuration with HeaderMongoMapper

The following is an example connector configuration containing the `namespace.mapper` property that
tells the connector to utilize the `HeaderMongoMapper`.

```JSON
{
  "name": "mongo-test",
  "config": {
    "topics":"end.to.end",
    "connector.class":"com.mongodb.kafka.connect.MongoSinkConnector",
    "connection.uri":"mongodb://localhost:27017",
    "key.converter":"io.confluent.connect.avro.AvroConverter",
    "key.converter.schema.registry.url":"http://localhost:8081",
    "value.converter":"io.confluent.connect.avro.AvroConverter",
    "value.converter.schema.registry.url":"http://localhost:8081",
    "namespace.mapper":"HeaderMongoMapper",
    "database":"default-sink",
    "collection":"default-collection",
    "document.id.strategy":"com.mongodb.kafka.connect.sink.processor.id.strategy.ProvidedInValueStrategy"
  }
}
```

## Configure Connector via REST

Run the following curl command from the `/config` directory containing `HeaderMapper.json`.

```bash
curl -X POST -H "Content-Type: application/json" --data @HeaderMapper.json http://localhost:8083/connectors
```

## Build Connector Locally

From the root of the `mongo-kafka` project, run -
 
```bash
./gradlew clean build createConfluentArchive
```

The connector artifacts will be placed into the `./build/confluentArchive` directory. 

Copy the `mongodb-kafka-connect-mongodb-(version)` directory into your Confluent Platform installation.

## Testing End to End

This functionality is tested end to end in `MongoSinkConnectorTest.testSinkSavesToMultipleNamespaces`.

If you would like to test it end to end against a local Confluent Platform,

1. Comment out the `@Ignore` on `MongoSinkConnectorTest.testSinkToConfluentPlatform`
2. Comment out the `@RegisterExtension` above the `KAFKA` property in `MongoKafkaTestCase`
    - With this in place, there will be a port conflict with your Confluent Platform broker
3. Run your local Mongo instance
4. Run your local Confluent Platform
5. Run the `testSinkToConfluentPlatform` test case
6. Validate results in Mongo using a tool like _MongoDB Compass Community_

## Live Debugging

If you're running Confluent Platform locally, you can debug the connector code in just a few steps.

```
> confluent stop connect
> export CONNECT_DEBUG=y; export DEBUG_SUSPEND_FLAG=y;
> confluent start connect

# attach to remote JVM in IntelliJ (host: localhost, port: 5005)
```

## Troubleshooting

### `confluent load` Not Working

For some reason `confluent load mongo -d HeaderMapper.properties` does not work. The `header.mapper` property is dropped.

Configuring the connector via REST seems to work fine though, so for now use that approach.
