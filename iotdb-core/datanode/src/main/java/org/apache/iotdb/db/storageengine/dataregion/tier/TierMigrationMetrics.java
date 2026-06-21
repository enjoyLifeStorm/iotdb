/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.storageengine.dataregion.tier;

import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.rescon.disk.TierManager;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

import java.util.List;

/**
 * Metrics collector for tiered storage migration. Collects metrics about migration tasks,
 * disk usage, and file counts across tiers.
 */
public class TierMigrationMetrics implements IMetricSet {

  private volatile long completedCount = 0;
  private volatile long failedCount = 0;
  private volatile long totalIoBytes = 0;

  public void incrementCompleted(long ioBytes) {
    completedCount++;
    totalIoBytes += ioBytes;
  }

  public void incrementFailed() {
    failedCount++;
  }

  @Override
  public void bindTo(AbstractMetricService metricService) {
    // Migration task count
    metricService.createAutoGauge(
        Metric.TIER_MIGRATION_TASK_COUNT.toString(),
        MetricLevel.CORE,
        TierMigrationManager.getInstance(),
        m -> m.isRunning() ? 1 : 0,
        Tag.STATUS.toString(),
        "running");

    // Migration completed count
    metricService.createAutoGauge(
        Metric.TIER_MIGRATION_COMPLETED.toString(),
        MetricLevel.CORE,
        this,
        m -> m.completedCount,
        Tag.NAME.toString(),
        "total");

    // Migration failed count
    metricService.createAutoGauge(
        Metric.TIER_MIGRATION_FAILED.toString(),
        MetricLevel.CORE,
        this,
        m -> m.failedCount,
        Tag.NAME.toString(),
        "total");

    // Migration IO bytes
    metricService.createAutoGauge(
        Metric.TIER_MIGRATION_IO_BYTES.toString(),
        MetricLevel.CORE,
        this,
        m -> m.totalIoBytes,
        Tag.NAME.toString(),
        "total");

    // Disk usage ratio per tier
    int tiersNum = TierManager.getInstance().getTiersNum();
    for (int i = 0; i < tiersNum; i++) {
      final int tierLevel = i;
      metricService.createAutoGauge(
          Metric.TIER_DISK_USAGE_RATIO.toString(),
          MetricLevel.CORE,
          TierManager.getInstance(),
          tm -> tm.getTierDiskUsage(tierLevel),
          Tag.TYPE.toString(),
          "tier_" + tierLevel);
    }

    // File count per tier
    for (int i = 0; i < tiersNum; i++) {
      final int tierLevel = i;
      metricService.createAutoGauge(
          Metric.TIER_FILE_COUNT.toString(),
          MetricLevel.CORE,
          this,
          m -> getFileCountForTier(tierLevel),
          Tag.TYPE.toString(),
          "tier_" + tierLevel);
    }
  }

  private long getFileCountForTier(int tierLevel) {
    long count = 0;
    for (DataRegion region : StorageEngine.getInstance().getAllDataRegions()) {
      for (TsFileResource resource : region.getAllClosedSequenceResources()) {
        if (resource.getTierLevel() == tierLevel) {
          count++;
        }
      }
      for (TsFileResource resource : region.getAllClosedUnsequenceResources()) {
        if (resource.getTierLevel() == tierLevel) {
          count++;
        }
      }
    }
    return count;
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    metricService.remove(MetricType.AUTO_GAUGE, Metric.TIER_MIGRATION_TASK_COUNT.toString());
    metricService.remove(MetricType.AUTO_GAUGE, Metric.TIER_MIGRATION_COMPLETED.toString());
    metricService.remove(MetricType.AUTO_GAUGE, Metric.TIER_MIGRATION_FAILED.toString());
    metricService.remove(MetricType.AUTO_GAUGE, Metric.TIER_MIGRATION_IO_BYTES.toString());
    metricService.remove(MetricType.AUTO_GAUGE, Metric.TIER_DISK_USAGE_RATIO.toString());
    metricService.remove(MetricType.AUTO_GAUGE, Metric.TIER_FILE_COUNT.toString());
  }
}
