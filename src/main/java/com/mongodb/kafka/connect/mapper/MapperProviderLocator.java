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

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.common.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.kafka.connect.sink.MongoSinkConfig;

public final class MapperProviderLocator {

  private static final Logger LOGGER = LoggerFactory.getLogger(MapperProviderLocator.class);
  private static final Map<String, MapperProvider> REGISTRY = new ConcurrentSkipListMap<>();

  static {
    loadMappers();
  }

  private MapperProviderLocator() {}

  private static void loadMappers() {
    LOGGER.debug("Loading all MapperProvider implementations on the classpath");

    final AtomicInteger count = new AtomicInteger();
    AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
      ServiceLoader<MapperProvider> providers = ServiceLoader.load(MapperProvider.class);

      try {
        providers.iterator().forEachRemaining(p -> {
          LOGGER.debug("Provider Located: '{}' -> {}", p.name(), p.getClass().getName());
          REGISTRY.put(p.name(), p);
          count.incrementAndGet();
        });

      } catch (Throwable t) {
        LOGGER.error(
            "Error loading mapper implementations. This could impact messages being delivered to MongoDB.",
            t
        );
      }

      return null;
    });

    LOGGER.debug("Registered {} mappers", count.get());
  }

  public static MongoMapperValidator validatorAndRecommender() {
    return new MongoMapperValidator(availableMappers());
  }

  public static Set<String> availableMappers() {
    return REGISTRY.keySet();
  }

  public static MongoMapper createMapper(
      final MongoSinkConfig connectorConfig, final String mapperName
  ) throws MapperNotFoundException, ConfigException {

    MapperProvider provider = REGISTRY.getOrDefault(mapperName, null);
    if (provider != null) {
      return provider.create(connectorConfig);
    }

    throw new MapperNotFoundException(
        String.format(
            "Unable to find the mapper '%s' in the set of loaded mappers: %s",
            mapperName,
            String.join(",", REGISTRY.keySet())
        )
    );
  }
}
