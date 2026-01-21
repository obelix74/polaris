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
package org.apache.polaris.core.persistence;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.List;
import java.util.Optional;
import org.apache.iceberg.metrics.CommitReport;
import org.apache.iceberg.metrics.ScanReport;

/**
 * Interface for persisting Iceberg metrics reports to a backend storage.
 *
 * <p>This interface abstracts metrics persistence operations, allowing different persistence
 * backends (JDBC, NoSQL, etc.) to provide their own implementations. Backends that do not support
 * metrics persistence should return a {@link #NOOP} implementation.
 *
 * <p>The interface uses Iceberg's native report types ({@link ScanReport}, {@link CommitReport})
 * along with contextual metadata to persist complete metrics information.
 *
 * @see ScanReport
 * @see CommitReport
 */
public interface MetricsPersistence {

  /**
   * A no-op implementation that discards all metrics. Use this for persistence backends that do not
   * support metrics storage.
   */
  MetricsPersistence NOOP = new NoOpMetricsPersistence();

  /**
   * Returns whether this persistence backend supports metrics storage.
   *
   * @return true if metrics can be persisted, false otherwise
   */
  default boolean isSupported() {
    return true;
  }

  /**
   * Persists a scan metrics report with full context information.
   *
   * @param scanReport the Iceberg scan report to persist
   * @param context the metrics context containing realm, catalog, principal, and trace information
   */
  void writeScanReport(@Nonnull ScanReport scanReport, @Nonnull MetricsContext context);

  /**
   * Persists a commit metrics report with full context information.
   *
   * @param commitReport the Iceberg commit report to persist
   * @param context the metrics context containing realm, catalog, principal, and trace information
   */
  void writeCommitReport(@Nonnull CommitReport commitReport, @Nonnull MetricsContext context);

  /**
   * Queries scan metrics reports for a specific table within a time range.
   *
   * @param catalogName the catalog name
   * @param namespace the namespace (dot-separated)
   * @param tableName the table name
   * @param startTimeMs start of time range (inclusive), or null for no lower bound
   * @param endTimeMs end of time range (exclusive), or null for no upper bound
   * @param limit maximum number of results to return
   * @return list of scan reports matching the criteria, or empty list if not supported
   */
  @Nonnull
  List<ScanReport> queryScanReports(
      @Nonnull String catalogName,
      @Nonnull String namespace,
      @Nonnull String tableName,
      @Nullable Long startTimeMs,
      @Nullable Long endTimeMs,
      int limit);

  /**
   * Queries commit metrics reports for a specific table within a time range.
   *
   * @param catalogName the catalog name
   * @param namespace the namespace (dot-separated)
   * @param tableName the table name
   * @param startTimeMs start of time range (inclusive), or null for no lower bound
   * @param endTimeMs end of time range (exclusive), or null for no upper bound
   * @param limit maximum number of results to return
   * @return list of commit reports matching the criteria, or empty list if not supported
   */
  @Nonnull
  List<CommitReport> queryCommitReports(
      @Nonnull String catalogName,
      @Nonnull String namespace,
      @Nonnull String tableName,
      @Nullable Long startTimeMs,
      @Nullable Long endTimeMs,
      int limit);

  /**
   * Queries scan metrics reports by OpenTelemetry trace ID.
   *
   * @param traceId the OpenTelemetry trace ID
   * @return list of scan reports with the given trace ID, or empty list if not supported
   */
  @Nonnull
  List<ScanReport> queryScanReportsByTraceId(@Nonnull String traceId);

  /**
   * Queries commit metrics reports by OpenTelemetry trace ID.
   *
   * @param traceId the OpenTelemetry trace ID
   * @return list of commit reports with the given trace ID, or empty list if not supported
   */
  @Nonnull
  List<CommitReport> queryCommitReportsByTraceId(@Nonnull String traceId);

  /**
   * Deletes scan metrics reports older than the specified timestamp.
   *
   * @param olderThanMs timestamp in milliseconds; reports older than this will be deleted
   * @return the number of reports deleted, or 0 if not supported
   */
  int deleteScanReportsOlderThan(long olderThanMs);

  /**
   * Deletes commit metrics reports older than the specified timestamp.
   *
   * @param olderThanMs timestamp in milliseconds; reports older than this will be deleted
   * @return the number of reports deleted, or 0 if not supported
   */
  int deleteCommitReportsOlderThan(long olderThanMs);

  /**
   * Deletes all metrics reports (scan and commit) older than the specified timestamp.
   *
   * @param olderThanMs timestamp in milliseconds; reports older than this will be deleted
   * @return the total number of reports deleted, or 0 if not supported
   */
  default int deleteAllReportsOlderThan(long olderThanMs) {
    return deleteScanReportsOlderThan(olderThanMs) + deleteCommitReportsOlderThan(olderThanMs);
  }
}

