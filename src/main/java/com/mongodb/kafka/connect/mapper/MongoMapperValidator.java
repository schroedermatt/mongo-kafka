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


package com.mongodb.kafka.connect.mapper;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

/**
 * Validates configuration values against the registered mapper providers.
 *
 * Recommends MongoMapper options based on the registered mapper providers.
 */
public class MongoMapperValidator implements ConfigDef.Validator, ConfigDef.Recommender {

  private final List<String> allowedValues;

  public MongoMapperValidator(final Collection<String> allowedValues) {
    this.allowedValues = new ArrayList<>();
    this.allowedValues.addAll(allowedValues);
  }

  @Override
  public void ensureValid(final String name, final Object value) {
    if (value == null || !allowedValues.contains(value.toString())) {
      throw new ConfigException(name, value, "Must be one of " + allowedValues);
    }
  }

  @Override
  public List<Object> validValues(final String name, final Map<String, Object> parsedConfig) {
    return Collections.unmodifiableList(new ArrayList<>(allowedValues));
  }

  @Override
  public boolean visible(final String name, final Map<String, Object> parsedConfig) {
    return true;
  }
}
