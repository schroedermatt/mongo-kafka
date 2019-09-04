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

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.ConnectException;

import com.mongodb.kafka.connect.mapper.MapperProvider;
import com.mongodb.kafka.connect.mapper.MongoMapper;
import com.mongodb.kafka.connect.sink.MongoSinkConfig;

public class KafkaTopicMapperProvider implements MapperProvider {

  @Override
  public String name() {
    return "KafkaTopicMapper";
  }

  @Override
  public Class<? extends MongoMapper> mapperClass() {
    return KafkaTopicMapper.class;
  }

  @Override
  public MongoMapper create(final MongoSinkConfig config) throws ConnectException, ConfigException {
    return new KafkaTopicMapper(config);
  }
}
