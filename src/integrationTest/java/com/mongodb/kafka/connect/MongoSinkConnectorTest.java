/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.mongodb.kafka.connect;

import static com.mongodb.kafka.connect.mongodb.MongoDBHelper.ALT_DATABASE_NAME;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Properties;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.mongodb.kafka.connect.avro.TweetMsg;
import com.mongodb.kafka.connect.mongodb.MongoKafkaTestCase;
import com.mongodb.kafka.connect.sink.MongoSinkTopicConfig;

class MongoSinkConnectorTest extends MongoKafkaTestCase {

    @Test
    @DisplayName("Ensure simple producer sends data")
    void testASimpleProducerSmokeTest() {
        KAFKA.createTopic(getTopicName());

        Properties props = new Properties();
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, getTopicName());
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.bootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        KafkaProducer<Integer, String> producer = new KafkaProducer<>(props);
        producer.initTransactions();
        producer.beginTransaction();

        IntStream.range(0, 10).forEach(i -> {
            producer.send(new ProducerRecord<>(getTopicName(), i, "Hello, World!"));
        });
        producer.commitTransaction();

        assertProduced(10);
    }

    @Test
    @DisplayName("Ensure sink connect saves data to MongoDB")
    void testSinkSavesAvroDataToMongoDB() {
        Stream<TweetMsg> tweets = IntStream.range(0, 100).mapToObj(i ->
                TweetMsg.newBuilder().setId$1(i)
                        .setText(format("test tweet %s end2end testing apache kafka <-> mongodb sink connector is fun!", i))
                        .setHashtags(asList(format("t%s", i), "kafka", "mongodb", "testing"))
                        .build()
        );

        KAFKA.createTopic(getTopicName());
        addSinkConnector();

        KafkaProducer<String, TweetMsg> producer = getProducer();
        producer.initTransactions();
        producer.beginTransaction();
        tweets.forEach(tweet -> producer.send(new ProducerRecord<>(getTopicName(), tweet)));
        producer.commitTransaction();

        assertProduced(100);
        assertEquals(100, getCollection().countDocuments());
    }

    @Test
    @DisplayName("Data is routed to multiple Mongo namespaces using record headers")
    void testSinkSavesToMultipleNamespaces() {
        String dbHeader = "MONGO_DB";
        String collHeader = "MONGO_COLL";
        String altCollection = "Other_Collection";

        Stream<TweetMsg> tweets = IntStream.range(0, 100).mapToObj(i ->
            TweetMsg.newBuilder().setId$1(i)
                .setText(format("test tweet %s end2end testing apache kafka <-> mongodb sink connector is fun!", i))
                .setHashtags(asList(format("t%s", i), "kafka", "mongodb", "testing"))
                .build()
        );

        KAFKA.createTopic(getTopicName());

        Properties overrides = new Properties();
        overrides.put(MongoSinkTopicConfig.DATABASE_HEADER_CONFIG, dbHeader);
        overrides.put(MongoSinkTopicConfig.COLLECTION_HEADER_CONFIG, collHeader);

        addSinkConnector(overrides);

        KafkaProducer<String, TweetMsg> producer = getProducer();
        producer.initTransactions();
        producer.beginTransaction();

        tweets.forEach(tweet -> {
            ProducerRecord<String, TweetMsg> record = new ProducerRecord<>(getTopicName(), tweet);
            // every 2 messages, save to the alternate collection
            if (tweet.getId$1() % 2 == 0) {
                record.headers().add(collHeader, altCollection.getBytes());
            }
            // every 5 messages, save to the alternate database
            if (tweet.getId$1() % 5 == 0) {
                record.headers().add(dbHeader, ALT_DATABASE_NAME.getBytes());
            }

            producer.send(record);
        });
        producer.commitTransaction();

        assertProduced(100);
        // default database, default collection
        assertEquals(40, getCollection().countDocuments());
        // default database, alternate collection
        assertEquals(40, getCollection(altCollection).countDocuments());
        // alternate database, default collection
        assertEquals(10, getMongoClient().getDatabase(ALT_DATABASE_NAME).getCollection(getCollectionName()).countDocuments());
        // alternate database, alternate collection
        assertEquals(10, getMongoClient().getDatabase(ALT_DATABASE_NAME).getCollection(altCollection).countDocuments());
    }

    private KafkaProducer<String, TweetMsg> getProducer() {
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, getTopicName());
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.bootstrapServers());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroSerializer");
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroSerializer");
        producerProps.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, KAFKA.schemaRegistryUrl());
        return new KafkaProducer<>(producerProps);
    }
}
