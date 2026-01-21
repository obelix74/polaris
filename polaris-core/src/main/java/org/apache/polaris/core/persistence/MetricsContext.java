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
import java.util.Optional;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.polaris.immutables.PolarisImmutable;

/**
 * Immutable context object containing metadata for metrics persistence.
 *
 * <p>This context captures all the contextual information needed to persist metrics reports,
 * including realm identification, catalog information, principal details, and OpenTelemetry trace
 * context for correlation.
 */
@PolarisImmutable
public interface MetricsContext {

  /** The realm identifier where this metrics report originated. */
  @Nonnull
  String realmId();

  /** The catalog ID (as string) where this metrics report originated. */
  @Nonnull
  Optional<String> catalogId();

  /** The catalog name where this metrics report originated. */
  @Nonnull
  String catalogName();

  /** The table identifier (namespace + table name) for this metrics report. */
  @Nonnull
  TableIdentifier tableIdentifier();

  /** The principal name that generated this metrics report (if available). */
  @Nonnull
  Optional<String> principalName();

  /** The request ID associated with this metrics report (if available). */
  @Nonnull
  Optional<String> requestId();

  /** The OpenTelemetry trace ID for correlation (if available). */
  @Nonnull
  Optional<String> otelTraceId();

  /** The OpenTelemetry span ID for correlation (if available). */
  @Nonnull
  Optional<String> otelSpanId();

  /**
   * Creates a new builder for MetricsContext.
   *
   * @return a new builder instance
   */
  static ImmutableMetricsContext.Builder builder() {
    return ImmutableMetricsContext.builder();
  }
}
