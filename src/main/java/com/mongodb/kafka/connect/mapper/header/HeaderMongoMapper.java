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

package com.mongodb.kafka.connect.mapper.header;

import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.SinkRecord;

import com.mongodb.MongoNamespace;

import com.mongodb.kafka.connect.mapper.MongoMapper;
import com.mongodb.kafka.connect.sink.MongoSinkConfig;
import com.mongodb.kafka.connect.sink.MongoSinkTopicConfig;

public class HeaderMongoMapper implements MongoMapper {
  public static final String NAME = "HeaderMongoMapper";
  public static final String DB_HEADER = "MONGO_DB";
  public static final String COLL_HEADER = "MONGO_COLL";

  private final String defaultDb;
  private final String defaultColl;

  public HeaderMongoMapper(final MongoSinkConfig config) {
    MongoSinkTopicConfig defaultConfig = config.getDefaultConfig();
    this.defaultDb = defaultConfig.getDatabase();
    this.defaultColl = defaultConfig.getNamespace().getCollectionName();
  }

  @Override
  public MongoNamespace mapToNamespace(final SinkRecord r) {
    // setup defaults if headers aren't on record
    String database = defaultDb;
    String collection = defaultColl;

    if (r.headers() != null) {
      boolean dbMatch = false;
      boolean collMatch = false;

      for (Header header : r.headers()) {
        if (!dbMatch && DB_HEADER.equals(header.key())) {
          database = header.value().toString();
          dbMatch = true;
        } else if (!collMatch && COLL_HEADER.equals(header.key())) {
          collection = header.value().toString();
          collMatch = true;
        }

        // break early if db and coll headers have already been found
        if (dbMatch && collMatch) {
          break;
        }
      }
    }

    return new MongoNamespace(database, collection);
  }
}
