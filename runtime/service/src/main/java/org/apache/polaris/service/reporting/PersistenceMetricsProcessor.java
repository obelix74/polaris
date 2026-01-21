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

import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.iceberg.metrics.CommitReport;
import org.apache.iceberg.metrics.ScanReport;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.persistence.ImmutableMetricsContext;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.core.persistence.MetricsContext;
import org.apache.polaris.core.persistence.MetricsPersistence;
import org.apache.polaris.persistence.relational.jdbc.JdbcBasePersistenceImpl;
import org.apache.polaris.persistence.relational.jdbc.JdbcMetricsPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link MetricsProcessor} that persists metrics to dedicated database tables.
 *
 * <p>This processor stores Iceberg metrics reports in dedicated tables:
 *
 * <ul>
 *   <li>{@code scan_metrics_report} - For ScanReport metrics
 *   <li>{@code commit_metrics_report} - For CommitReport metrics
 * </ul>
 *
 * <p>The processor includes full context information such as realm ID, catalog ID, principal name,
 * request ID, and OpenTelemetry trace context for correlation and analysis.
 *
 * <p>This implementation uses the {@link MetricsPersistence} interface to abstract the persistence
 * backend, allowing different backends (JDBC, NoSQL, etc.) to provide their own implementations.
 * Backends that do not support metrics persistence will receive a no-op implementation that
 * silently discards metrics.
 *
 * <p>Configuration:
 *
 * <pre>
 * polaris:
 *   metrics:
 *     processor:
 *       type: persistence
 *       retention:
 *         enabled: true
 *         retention-period: P30D
 *         cleanup-interval: PT6H
 * </pre>
 */
@ApplicationScoped
@Identifier("persistence")
public class PersistenceMetricsProcessor implements MetricsProcessor {

  private static final Logger LOGGER = LoggerFactory.getLogger(PersistenceMetricsProcessor.class);

  private final MetaStoreManagerFactory metaStoreManagerFactory;

  @Inject
  public PersistenceMetricsProcessor(MetaStoreManagerFactory metaStoreManagerFactory) {
    this.metaStoreManagerFactory = metaStoreManagerFactory;
    LOGGER.info("PersistenceMetricsProcessor initialized - metrics will be persisted to database");
  }

  @Override
  public void process(MetricsProcessingContext context) {
    try {
      // Get the MetricsPersistence for the realm
      MetricsPersistence metricsPersistence = getMetricsPersistence(context.realmId());

      // Check if persistence is supported
      if (!metricsPersistence.isSupported()) {
        LOGGER.debug(
            "Metrics persistence not supported for realm: {}. Metrics will not be persisted.",
            context.realmId());
        return;
      }

      // Build the metrics context
      MetricsContext metricsContext = buildMetricsContext(context);

      // Persist based on report type
      if (context.metricsReport() instanceof ScanReport scanReport) {
        metricsPersistence.writeScanReport(scanReport, metricsContext);
        LOGGER.debug(
            "Persisted scan metrics for {}.{}", context.catalogName(), context.tableIdentifier());
      } else if (context.metricsReport() instanceof CommitReport commitReport) {
        metricsPersistence.writeCommitReport(commitReport, metricsContext);
        LOGGER.debug(
            "Persisted commit metrics for {}.{}", context.catalogName(), context.tableIdentifier());
      } else {
        LOGGER.warn(
            "Unknown metrics report type: {}. Metrics will not be persisted.",
            context.metricsReport().getClass().getName());
      }
    } catch (Exception e) {
      LOGGER.error(
          "Failed to persist metrics for {}.{}: {}",
          context.catalogName(),
          context.tableIdentifier(),
          e.getMessage(),
          e);
    }
  }

  /**
   * Gets the appropriate MetricsPersistence implementation for the given realm.
   *
   * <p>This method creates the MetricsPersistence based on the underlying persistence backend. For
   * JDBC backends, it returns a JdbcMetricsPersistence. For other backends, it returns the no-op
   * implementation.
   *
   * @param realmId the realm identifier
   * @return the MetricsPersistence implementation for the realm
   */
  private MetricsPersistence getMetricsPersistence(String realmId) {
    RealmContext realmContext = () -> realmId;
    var session = metaStoreManagerFactory.getOrCreateSession(realmContext);

    if (session instanceof JdbcBasePersistenceImpl jdbcPersistence) {
      return new JdbcMetricsPersistence(jdbcPersistence);
    }

    return MetricsPersistence.NOOP;
  }

  /**
   * Builds a MetricsContext from the MetricsProcessingContext.
   *
   * @param context the metrics processing context
   * @return the metrics context for persistence
   */
  private MetricsContext buildMetricsContext(MetricsProcessingContext context) {
    return ImmutableMetricsContext.builder()
        .realmId(context.realmId())
        .catalogId(context.catalogId().map(String::valueOf))
        .catalogName(context.catalogName())
        .tableIdentifier(context.tableIdentifier())
        .principalName(context.principalName())
        .requestId(context.requestId())
        .otelTraceId(context.otelTraceId())
        .otelSpanId(context.otelSpanId())
        .build();
  }
}
