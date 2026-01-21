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
import java.util.Collections;
import java.util.List;
import org.apache.iceberg.metrics.CommitReport;
import org.apache.iceberg.metrics.ScanReport;

/**
 * A no-op implementation of {@link MetricsPersistence} that discards all metrics.
 *
 * <p>This implementation is used for persistence backends that do not support metrics storage, such
 * as in-memory backends or NoSQL backends that haven't implemented metrics tables yet.
 *
 * <p>All write operations are silently ignored, and all query operations return empty results.
 */
final class NoOpMetricsPersistence implements MetricsPersistence {

  NoOpMetricsPersistence() {
    // Package-private constructor - use MetricsPersistence.NOOP
  }

  @Override
  public boolean isSupported() {
    return false;
  }

  @Override
  public void writeScanReport(@Nonnull ScanReport scanReport, @Nonnull MetricsContext context) {
    // No-op: metrics are discarded
  }

  @Override
  public void writeCommitReport(@Nonnull CommitReport commitReport, @Nonnull MetricsContext context) {
    // No-op: metrics are discarded
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
    return Collections.emptyList();
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
    return Collections.emptyList();
  }

  @Nonnull
  @Override
  public List<ScanReport> queryScanReportsByTraceId(@Nonnull String traceId) {
    return Collections.emptyList();
  }

  @Nonnull
  @Override
  public List<CommitReport> queryCommitReportsByTraceId(@Nonnull String traceId) {
    return Collections.emptyList();
  }

  @Override
  public int deleteScanReportsOlderThan(long olderThanMs) {
    return 0;
  }

  @Override
  public int deleteCommitReportsOlderThan(long olderThanMs) {
    return 0;
  }
}

