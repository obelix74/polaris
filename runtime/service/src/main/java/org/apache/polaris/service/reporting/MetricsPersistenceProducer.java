/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.polaris.service.reporting;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.context.RequestScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.persistence.BasePersistence;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.core.persistence.MetricsPersistence;
import org.apache.polaris.persistence.relational.jdbc.JdbcBasePersistenceImpl;
import org.apache.polaris.persistence.relational.jdbc.JdbcMetricsPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CDI producer for {@link MetricsPersistence} instances.
 *
 * <p>This producer creates the appropriate {@link MetricsPersistence} implementation based on the
 * underlying persistence backend:
 *
 * <ul>
 *   <li>For JDBC backends: Returns a {@link JdbcMetricsPersistence} that persists metrics to
 *       database tables
 *   <li>For other backends (in-memory, NoSQL): Returns {@link MetricsPersistence#NOOP} which
 *       discards metrics
 * </ul>
 *
 * <p>This approach eliminates the need for {@code instanceof} checks in service code, following the
 * principle of using CDI beans for backend-specific functionality.
 */
@ApplicationScoped
public class MetricsPersistenceProducer {

  private static final Logger LOGGER = LoggerFactory.getLogger(MetricsPersistenceProducer.class);

  private final MetaStoreManagerFactory metaStoreManagerFactory;

  @Inject
  public MetricsPersistenceProducer(MetaStoreManagerFactory metaStoreManagerFactory) {
    this.metaStoreManagerFactory = metaStoreManagerFactory;
  }

  /**
   * Produces a {@link MetricsPersistence} instance for the current request's realm.
   *
   * <p>The implementation returned depends on the persistence backend configured for the realm:
   *
   * <ul>
   *   <li>JDBC backend: Returns {@link JdbcMetricsPersistence}
   *   <li>Other backends: Returns {@link MetricsPersistence#NOOP}
   * </ul>
   *
   * @param realmContext the current realm context
   * @return the appropriate MetricsPersistence implementation
   */
  @Produces
  @RequestScoped
  public MetricsPersistence metricsPersistence(RealmContext realmContext) {
    BasePersistence session = metaStoreManagerFactory.getOrCreateSession(realmContext);

    if (session instanceof JdbcBasePersistenceImpl jdbcPersistence) {
      LOGGER.debug(
          "Creating JdbcMetricsPersistence for realm: {}", realmContext.getRealmIdentifier());
      return new JdbcMetricsPersistence(jdbcPersistence);
    }

    LOGGER.debug(
        "Persistence backend {} does not support metrics persistence for realm: {}. "
            + "Using no-op implementation.",
        session.getClass().getSimpleName(),
        realmContext.getRealmIdentifier());
    return MetricsPersistence.NOOP;
  }
}

