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
package org.apache.polaris.persistence.relational.jdbc;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import org.apache.iceberg.metrics.CommitReport;
import org.apache.iceberg.metrics.ScanReport;
import org.apache.polaris.core.persistence.MetricsContext;
import org.apache.polaris.core.persistence.MetricsPersistence;
import org.apache.polaris.persistence.relational.jdbc.models.MetricsReportConverter;
import org.apache.polaris.persistence.relational.jdbc.models.ModelCommitMetricsReport;
import org.apache.polaris.persistence.relational.jdbc.models.ModelScanMetricsReport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JDBC implementation of {@link MetricsPersistence} that persists metrics to relational database
 * tables.
 *
 * <p>This implementation stores Iceberg metrics reports in dedicated tables:
 *
 * <ul>
 *   <li>{@code scan_metrics_report} - For ScanReport metrics
 *   <li>{@code commit_metrics_report} - For CommitReport metrics
 * </ul>
 *
 * <p>The implementation delegates to {@link JdbcBasePersistenceImpl} for actual database
 * operations.
 */
public class JdbcMetricsPersistence implements MetricsPersistence {

  private static final Logger LOGGER = LoggerFactory.getLogger(JdbcMetricsPersistence.class);

  private final JdbcBasePersistenceImpl jdbcPersistence;

  /**
   * Creates a new JdbcMetricsPersistence instance.
   *
   * @param jdbcPersistence the JDBC persistence implementation to delegate to
   */
  public JdbcMetricsPersistence(@Nonnull JdbcBasePersistenceImpl jdbcPersistence) {
    this.jdbcPersistence = jdbcPersistence;
  }

  @Override
  public void writeScanReport(@Nonnull ScanReport scanReport, @Nonnull MetricsContext context) {
    try {
      String namespace = context.tableIdentifier().namespace().toString();
      String catalogId = context.catalogId().orElse(null);

      ModelScanMetricsReport modelReport =
          MetricsReportConverter.fromScanReport(
              scanReport,
              context.realmId(),
              catalogId,
              context.catalogName(),
              namespace,
              context.principalName().orElse(null),
              context.requestId().orElse(null),
              context.otelTraceId().orElse(null),
              context.otelSpanId().orElse(null));

      jdbcPersistence.writeScanMetricsReport(modelReport);
      LOGGER.debug(
          "Persisted scan metrics for {}.{}", context.catalogName(), context.tableIdentifier());
    } catch (Exception e) {
      LOGGER.error("Failed to persist scan metrics: {}", e.getMessage(), e);
      throw new RuntimeException("Failed to persist scan metrics", e);
    }
  }

  @Override
  public void writeCommitReport(
      @Nonnull CommitReport commitReport, @Nonnull MetricsContext context) {
    try {
      String namespace = context.tableIdentifier().namespace().toString();
      String catalogId = context.catalogId().orElse(null);

      ModelCommitMetricsReport modelReport =
          MetricsReportConverter.fromCommitReport(
              commitReport,
              context.realmId(),
              catalogId,
              context.catalogName(),
              namespace,
              context.principalName().orElse(null),
              context.requestId().orElse(null),
              context.otelTraceId().orElse(null),
              context.otelSpanId().orElse(null));

      jdbcPersistence.writeCommitMetricsReport(modelReport);
      LOGGER.debug(
          "Persisted commit metrics for {}.{}", context.catalogName(), context.tableIdentifier());
    } catch (Exception e) {
      LOGGER.error("Failed to persist commit metrics: {}", e.getMessage(), e);
      throw new RuntimeException("Failed to persist commit metrics", e);
    }
  }

  @Nonnull
  @Override
  public List<ScanReport> queryScanReports(
      @Nonnull String catalogName,
      @Nonnull String namespace,
      @Nonnull String tableName,
      @Nullable Long startTimeMs,
      @Nullable Long endTimeMs,
      int limit) {
    try {
      List<ModelScanMetricsReport> modelReports =
          jdbcPersistence.queryScanMetricsReports(
              catalogName, namespace, tableName, startTimeMs, endTimeMs, limit);
      // Note: Converting back to ScanReport would require additional work
      // For now, return empty list - query functionality can be added later
      LOGGER.debug("Query scan reports returned {} results", modelReports.size());
      return Collections.emptyList();
    } catch (Exception e) {
      LOGGER.error("Failed to query scan metrics: {}", e.getMessage(), e);
      return Collections.emptyList();
    }
  }

  @Nonnull
  @Override
  public List<CommitReport> queryCommitReports(
      @Nonnull String catalogName,
      @Nonnull String namespace,
      @Nonnull String tableName,
      @Nullable Long startTimeMs,
      @Nullable Long endTimeMs,
      int limit) {
    try {
      List<ModelCommitMetricsReport> modelReports =
          jdbcPersistence.queryCommitMetricsReports(
              catalogName, namespace, tableName, startTimeMs, endTimeMs, limit);
      LOGGER.debug("Query commit reports returned {} results", modelReports.size());
      return Collections.emptyList();
    } catch (Exception e) {
      LOGGER.error("Failed to query commit metrics: {}", e.getMessage(), e);
      return Collections.emptyList();
    }
  }

  @Nonnull
  @Override
  public List<ScanReport> queryScanReportsByTraceId(@Nonnull String traceId) {
    try {
      List<ModelScanMetricsReport> modelReports =
          jdbcPersistence.queryScanMetricsReportsByTraceId(traceId);
      LOGGER.debug("Query scan reports by trace ID returned {} results", modelReports.size());
      return Collections.emptyList();
    } catch (Exception e) {
      LOGGER.error("Failed to query scan metrics by trace ID: {}", e.getMessage(), e);
      return Collections.emptyList();
    }
  }

  @Nonnull
  @Override
  public List<CommitReport> queryCommitReportsByTraceId(@Nonnull String traceId) {
    try {
      List<ModelCommitMetricsReport> modelReports =
          jdbcPersistence.queryCommitMetricsReportsByTraceId(traceId);
      LOGGER.debug("Query commit reports by trace ID returned {} results", modelReports.size());
      return Collections.emptyList();
    } catch (Exception e) {
      LOGGER.error("Failed to query commit metrics by trace ID: {}", e.getMessage(), e);
      return Collections.emptyList();
    }
  }

  @Override
  public int deleteScanReportsOlderThan(long olderThanMs) {
    try {
      int deleted = jdbcPersistence.deleteScanMetricsReportsOlderThan(olderThanMs);
      LOGGER.debug("Deleted {} scan metrics reports older than {}", deleted, olderThanMs);
      return deleted;
    } catch (Exception e) {
      LOGGER.error("Failed to delete old scan metrics: {}", e.getMessage(), e);
      return 0;
    }
  }

  @Override
  public int deleteCommitReportsOlderThan(long olderThanMs) {
    try {
      int deleted = jdbcPersistence.deleteCommitMetricsReportsOlderThan(olderThanMs);
      LOGGER.debug("Deleted {} commit metrics reports older than {}", deleted, olderThanMs);
      return deleted;
    } catch (Exception e) {
      LOGGER.error("Failed to delete old commit metrics: {}", e.getMessage(), e);
      return 0;
    }
  }
}
