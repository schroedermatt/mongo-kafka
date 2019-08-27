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
 *
 * Original Work: Apache License, Version 2.0, Copyright 2017 Hans-Peter Grahsl.
 */

package com.mongodb.kafka.connect.sink;

import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.MAX_NUM_RETRIES_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.RETRIES_DEFER_TIMEOUT_CONFIG;
import static com.mongodb.kafka.connect.util.ConfigHelper.getMongoDriverInformation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.bson.BsonDocument;

import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoException;
import com.mongodb.MongoNamespace;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.WriteModel;

import com.mongodb.kafka.connect.Versions;
import com.mongodb.kafka.connect.sink.converter.SinkConverter;
import com.mongodb.kafka.connect.sink.converter.SinkDocument;
import com.mongodb.kafka.connect.sink.processor.PostProcessors;
import com.mongodb.kafka.connect.sink.writemodel.strategy.WriteModelStrategy;

public class MongoSinkTask extends SinkTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(MongoSinkTask.class);
    private static final BulkWriteOptions BULK_WRITE_OPTIONS = new BulkWriteOptions();

    private MongoSinkConfig sinkConfig;
    private MongoClient mongoClient;
    private Map<String, AtomicInteger> remainingRetriesTopicMap;

    private SinkConverter sinkConverter = new SinkConverter();

    @Override
    public String version() {
        return Versions.VERSION;
    }

    /**
     * Start the Task. This should handle any configuration parsing and one-time setup of the task.
     * @param props initial configuration
     */
    @Override
    public void start(final Map<String, String> props) {
        LOGGER.info("Starting MongoDB sink task");
        try {
            sinkConfig = new MongoSinkConfig(props);
            remainingRetriesTopicMap = new ConcurrentHashMap<>(sinkConfig.getTopics().stream().collect(Collectors.toMap((t) -> t,
                            (t) -> new AtomicInteger(sinkConfig.getMongoSinkTopicConfig(t).getInt(MAX_NUM_RETRIES_CONFIG)))));
        } catch (Exception e) {
            throw new ConnectException("Failed to start new task", e);
        }
        LOGGER.debug("Started MongoDB sink task");
    }

    /**
     * Put the records in the sink. Usually this should send the records to the sink asynchronously
     * and immediately return.
     *
     * If this operation fails, the SinkTask may throw a {@link org.apache.kafka.connect.errors.RetriableException} to
     * indicate that the framework should attempt to retry the same call again. Other exceptions will cause the task to
     * be stopped immediately. {@link SinkTaskContext#timeout(long)} can be used to set the maximum time before the
     * batch will be retried.
     *
     * @param records the set of records to send
     */
    @Override
    public void put(final Collection<SinkRecord> records) {
        if (records.isEmpty()) {
            LOGGER.debug("No sink records to process for current poll operation");
            return;
        }

        LOGGER.debug("Number of sink records to process: {}", records.size());

        Map<MongoNamespace, RecordBatches> namespaceBatches = createSinkRecordBatchesPerNamespace(records);
        namespaceBatches.forEach((namespace, batches) -> {
            MongoSinkTopicConfig batchConfig = batches.getConfig();
            batches.getBufferedBatches().forEach(batch -> {
                    processSinkRecords(batchConfig, namespace, batch);

                    RateLimitSettings rls = batchConfig.getRateLimitSettings();
                    if (rls.isTriggered()) {
                        LOGGER.debug("Rate limit settings triggering {}ms defer timeout after processing {}"
                            + " further batches for topic {}",
                            rls.getTimeoutMs(),
                            rls.getEveryN(),
                            batches.getConfig().getTopic());
                        try {
                            Thread.sleep(rls.getTimeoutMs());
                        } catch (InterruptedException e) {
                            LOGGER.error(e.getMessage());
                        }
                    }
                }
            );
        });
    }

    /**
     * Flush all records that have been {@link #put(Collection)} for the specified topic-partitions.
     *
     * @param currentOffsets the current offset state as of the last call to {@link #put(Collection)}},
     *                       provided for convenience but could also be determined by tracking all offsets included in the
     *                       {@link SinkRecord}s passed to {@link #put}.
     */
    @Override
    public void flush(final Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        //NOTE: flush is not used for now...
        LOGGER.debug("Flush called - noop");
    }

    /**
     * Perform any cleanup to stop this task. In SinkTasks, this method is invoked only once outstanding calls to other
     * methods have completed (e.g., {@link #put(Collection)} has returned) and a final {@link #flush(Map)} and offset
     * commit has completed. Implementations of this method should only need to perform final cleanup operations, such
     * as closing network connections to the sink system.
     */
    @Override
    public void stop() {
        LOGGER.info("Stopping MongoDB sink task");
        if (mongoClient != null) {
            mongoClient.close();
        }
    }

    private MongoClient getMongoClient() {
        if (mongoClient == null) {
            mongoClient = MongoClients.create(sinkConfig.getConnectionString(), getMongoDriverInformation());
        }
        return mongoClient;
    }

    private void processSinkRecords(final MongoSinkTopicConfig config, final MongoNamespace namespace, final List<SinkRecord> batch) {
        List<? extends WriteModel<BsonDocument>> writeModels = config.getCdcHandler().isPresent()
                ? buildWriteModelCDC(config, batch) : buildWriteModel(config, batch);
        try {
            if (!writeModels.isEmpty()) {
                LOGGER.debug("Bulk writing {} document(s) into collection [{}]",
                    writeModels.size(),
                    namespace.getFullName());

                BulkWriteResult result = getMongoClient()
                        .getDatabase(namespace.getDatabaseName())
                        .getCollection(namespace.getCollectionName(), BsonDocument.class)
                        .bulkWrite(writeModels, BULK_WRITE_OPTIONS);
                LOGGER.debug("Mongodb bulk write result: {}", result);
            }
        } catch (MongoBulkWriteException e) {
            LOGGER.error("Mongodb bulk write (partially) failed", e);
            LOGGER.error(e.getWriteResult().toString());
            LOGGER.error(e.getWriteErrors().toString());
            LOGGER.error(e.getWriteConcernError().toString());
            checkRetriableException(config.getTopic(), e, config.getInt(RETRIES_DEFER_TIMEOUT_CONFIG));
        } catch (MongoException e) {
            LOGGER.error("Error on mongodb operation", e);
            LOGGER.error("Writing {} document(s) into collection [{}] failed -> remaining retries ({})",
                    writeModels.size(), namespace.getFullName(), remainingRetriesTopicMap.get(config.getTopic()).get());
            checkRetriableException(config.getTopic(), e, config.getInt(RETRIES_DEFER_TIMEOUT_CONFIG));
        }
    }

    private void checkRetriableException(final String key, final MongoException e, final Integer deferRetryMs) {
        if (remainingRetriesTopicMap.get(key).decrementAndGet() <= 0) {
            throw new DataException("Failed to write mongodb documents despite retrying", e);
        }
        LOGGER.debug("Deferring retry operation for {}ms", deferRetryMs);
        context.timeout(deferRetryMs);
        throw new RetriableException(e.getMessage(), e);
    }

    Map<MongoNamespace, RecordBatches> createSinkRecordBatchesPerNamespace(final Collection<SinkRecord> records) {
        Map<MongoNamespace, RecordBatches> batchMapping = new HashMap<>();

        LOGGER.debug("Buffering sink records into batches for each Mongo namespace (database/collection)");

        records.forEach(r -> {
            MongoSinkTopicConfig config;
            MongoNamespace namespace;

            if (sinkConfig.inspectHeaders()) {
                config = sinkConfig.getDefaultConfig();
                String database = config.getDatabase();
                String collection = config.getNamespace().getCollectionName();

                // check headers for an overriding value for db and/or coll
                boolean dbMatch = false;
                boolean collMatch = false;
                if (r.headers() != null) {
                    for (Header header : r.headers()) {
                        if (!dbMatch && sinkConfig.getDatabaseHeader().equals(header.key())) {
                            database = header.value().toString();
                            dbMatch = true;
                        } else if (!collMatch && sinkConfig.getCollectionHeader().equals(header.key())) {
                            collection = header.value().toString();
                            collMatch = true;
                        }

                        // break early if db and coll headers have already been found
                        if (dbMatch && collMatch) {
                            break;
                        }
                    }
                }

                namespace = new MongoNamespace(database, collection);
            } else {
                config = sinkConfig.getMongoSinkTopicConfig(r.topic());
                namespace = config.getNamespace();
            }

            RecordBatches batches = batchMapping.get(namespace);
            if (batches == null) {
                LOGGER.debug("Batch size for collection {} is at most {} record(s)",
                    namespace.getCollectionName(), config.getMaxBatchSize());
                batches = new RecordBatches(config, records.size());
                batchMapping.put(namespace, batches);
            }
            batches.buffer(r);
        });

        return batchMapping;
    }

    List<? extends WriteModel<BsonDocument>> buildWriteModel(final MongoSinkTopicConfig config, final Collection<SinkRecord> records) {
        List<WriteModel<BsonDocument>> docsToWrite = new ArrayList<>(records.size());
        LOGGER.debug("building write model for {} record(s)", records.size());

        PostProcessors postProcessors = config.getPostProcessors();
        records.forEach(record -> {
                    SinkDocument doc = sinkConverter.convert(record);
                    postProcessors.getPostProcessorList().forEach(pp -> pp.process(doc, record));

                    if (doc.getValueDoc().isPresent()) {
                        docsToWrite.add(config.getWriteModelStrategy().createWriteModel(doc));
                    } else {
                        Optional<WriteModelStrategy> deleteOneModelWriteStrategy = config.getDeleteOneWriteModelStrategy();
                        if (doc.getKeyDoc().isPresent() && deleteOneModelWriteStrategy.isPresent()) {
                            docsToWrite.add(deleteOneModelWriteStrategy.get().createWriteModel(doc));
                        } else {
                            LOGGER.error("skipping sink record {} for which neither key doc nor value doc were present", record);
                        }
                    }
                }
        );
        return docsToWrite;
    }

    List<? extends WriteModel<BsonDocument>> buildWriteModelCDC(final MongoSinkTopicConfig config, final Collection<SinkRecord> records) {
        LOGGER.debug("Building CDC write model for {} record(s) for topic {}", records.size(), config.getTopic());
        return records.stream()
                .map(sinkConverter::convert)
                .map(sd -> config.getCdcHandler().flatMap(c -> c.handle(sd)))
                .flatMap(o -> o.map(Stream::of).orElseGet(Stream::empty))
                .collect(Collectors.toList());
    }
}
