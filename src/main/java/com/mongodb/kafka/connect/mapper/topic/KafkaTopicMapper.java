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

package com.mongodb.kafka.connect.mapper.topic;

import org.apache.kafka.connect.sink.SinkRecord;

import com.mongodb.MongoNamespace;

import com.mongodb.kafka.connect.mapper.MongoMapper;
import com.mongodb.kafka.connect.sink.MongoSinkConfig;
import com.mongodb.kafka.connect.sink.MongoSinkTopicConfig;

public class KafkaTopicMapper implements MongoMapper {
  private final MongoSinkConfig config;

  public KafkaTopicMapper(final MongoSinkConfig config) {
    this.config = config;
  }

  @Override
  public MongoNamespace mapToNamespace(final SinkRecord r) {
    MongoSinkTopicConfig topicConfig = this.config.getMongoSinkTopicConfig(r.topic());
    return topicConfig.getNamespace();
  }
}
