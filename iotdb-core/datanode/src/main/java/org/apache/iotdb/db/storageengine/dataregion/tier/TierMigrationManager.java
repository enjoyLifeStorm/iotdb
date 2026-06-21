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

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.service.IService;
import org.apache.iotdb.commons.service.ServiceType;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.db.storageengine.rescon.disk.TierManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Manager for tiered storage migration. Periodically scans TsFiles and migrates them to lower tiers
 * based on TTL and disk space thresholds.
 */
public class TierMigrationManager implements IService {
  private static final Logger LOGGER = LoggerFactory.getLogger(TierMigrationManager.class);

  private static final TierMigrationManager INSTANCE = new TierMigrationManager();

  private final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private ScheduledExecutorService scheduler;
  private ScheduledFuture<?> scheduledTask;
  private volatile boolean isRunning = false;

  private TierMigrationManager() {}

  public static TierMigrationManager getInstance() {
    return INSTANCE;
  }

  @Override
  public void start() {
    LOGGER.info("TierMigration: start() called, enableTieredStorage={}", config.isEnableTieredStorage());
    if (!config.isEnableTieredStorage()) {
      LOGGER.info("TierMigration: Tiered storage is disabled, skipping start");
      return;
    }

    if (isRunning) {
      return;
    }

    scheduler =
        IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor(
            ThreadName.TIER_MIGRATION_SCHEDULER.getName());

    final int intervalSeconds = config.getTierMigrationIntervalSeconds();
    LOGGER.info("TierMigration: Starting with interval {} seconds", intervalSeconds);
    scheduledTask =
        scheduler.scheduleWithFixedDelay(
            this::runMigrationCycle,
            intervalSeconds,
            intervalSeconds,
            TimeUnit.SECONDS);

    isRunning = true;
    LOGGER.info("TierMigration: Started with interval {} seconds", intervalSeconds);
  }

  @Override
  public void stop() {
    if (!isRunning) {
      return;
    }

    if (scheduledTask != null) {
      scheduledTask.cancel(false);
    }

    if (scheduler != null) {
      scheduler.shutdown();
      try {
        if (!scheduler.awaitTermination(30, TimeUnit.SECONDS)) {
          scheduler.shutdownNow();
        }
      } catch (InterruptedException e) {
        scheduler.shutdownNow();
        Thread.currentThread().interrupt();
      }
    }

    isRunning = false;
    LOGGER.info("TierMigration: Stopped");
  }

  @Override
  public ServiceType getID() {
    return ServiceType.TIER_MIGRATION_SERVICE;
  }

  private void runMigrationCycle() {
    try {
      final int maxTierLevel = TierManager.getInstance().getMaxTierLevel();
      final int tiersNum = TierManager.getInstance().getTiersNum();
      LOGGER.info("TierMigration: runMigrationCycle called, maxTierLevel={}, tiersNum={}", maxTierLevel, tiersNum);
      if (maxTierLevel <= 0) {
        LOGGER.info("TierMigration: maxTierLevel <= 0, skipping migration cycle");
        return;
      }

      int migratedCount = 0;
      final int batchSize = config.getTierMigrationBatchSize();
      final long[] tierTTL = CommonDescriptor.getInstance().getConfig().getTierTTLInMs();
      final double[] spaceThresholds = config.getTierSpaceUsageThresholds();
      LOGGER.info("TierMigration: batchSize={}, tierTTL={}, spaceThresholds={}", 
          batchSize, 
          tierTTL != null ? java.util.Arrays.toString(tierTTL) : "null",
          spaceThresholds != null ? java.util.Arrays.toString(spaceThresholds) : "null");

      // Check each tier for files that need migration
      for (int tierLevel = 0; tierLevel < maxTierLevel; tierLevel++) {
        if (migratedCount >= batchSize) {
          break;
        }

        // Check TTL-based migration
        if (tierTTL != null && tierLevel < tierTTL.length && tierTTL[tierLevel] > 0) {
          migratedCount += migrateByTTL(tierLevel, tierTTL[tierLevel], batchSize - migratedCount);
        }

        // Check space-based migration
        if (spaceThresholds != null && tierLevel < spaceThresholds.length) {
          migratedCount +=
              migrateBySpace(tierLevel, spaceThresholds[tierLevel], batchSize - migratedCount);
        }
      }

      if (migratedCount > 0) {
        LOGGER.info("TierMigration: Migration cycle completed, migrated {} files", migratedCount);
      }

    } catch (Exception e) {
      LOGGER.error("TierMigration: Error during migration cycle", e);
    }
  }

  private int migrateByTTL(int tierLevel, long ttlMs, int maxCount) {
    int migratedCount = 0;
    final long now = System.currentTimeMillis();
    final List<TsFileResource> candidates = new ArrayList<>();

    // Collect candidates from all DataRegions
    for (DataRegion region : StorageEngine.getInstance().getAllDataRegions()) {
      if (migratedCount + candidates.size() >= maxCount) {
        break;
      }

      for (TsFileResource resource : region.getAllClosedSequenceResources()) {
        if (migratedCount + candidates.size() >= maxCount) {
          break;
        }
        if (shouldMigrateByTTL(resource, tierLevel, now, ttlMs)) {
          candidates.add(resource);
        }
      }

      for (TsFileResource resource : region.getAllClosedUnsequenceResources()) {
        if (migratedCount + candidates.size() >= maxCount) {
          break;
        }
        if (shouldMigrateByTTL(resource, tierLevel, now, ttlMs)) {
          candidates.add(resource);
        }
      }
    }

    // Submit migration tasks
    for (TsFileResource resource : candidates) {
      if (resource.transformStatus(TsFileResourceStatus.MIGRATING)) {
        // Revert status - the task will set it again
        resource.transformStatus(TsFileResourceStatus.NORMAL);

        final TierMigrationTask task =
            new TierMigrationTask(resource, tierLevel + 1, resource.isSeq());
        // Execute synchronously for now to avoid thread pool complexity
        task.run();
        migratedCount++;
      }
    }

    return migratedCount;
  }

  private boolean shouldMigrateByTTL(
      TsFileResource resource, int tierLevel, long now, long ttlMs) {
    if (resource.getTierLevel() != tierLevel) {
      return false;
    }

    if (resource.getStatus() != TsFileResourceStatus.NORMAL) {
      return false;
    }

    if (!resource.isClosed()) {
      return false;
    }

    // Check if file's end time is older than TTL
    final long fileEndTime = resource.getFileEndTime();
    if (fileEndTime == Long.MIN_VALUE || fileEndTime == Long.MAX_VALUE) {
      return false;
    }

    return (now - fileEndTime) > ttlMs;
  }

  private int migrateBySpace(int tierLevel, double threshold, int maxCount) {
    // Check disk usage for this tier
    final double diskUsage = TierManager.getInstance().getTierDiskUsage(tierLevel);
    if (diskUsage <= threshold) {
      return 0;
    }

    LOGGER.info(
        "TierMigration: Tier {} disk usage {} exceeds threshold {}, starting space-based migration",
        tierLevel,
        diskUsage,
        threshold);

    // For space-based migration, we migrate the oldest files
    int migratedCount = 0;
    final List<TsFileResource> candidates = new ArrayList<>();

    for (DataRegion region : StorageEngine.getInstance().getAllDataRegions()) {
      if (migratedCount + candidates.size() >= maxCount) {
        break;
      }

      for (TsFileResource resource : region.getAllClosedSequenceResources()) {
        if (migratedCount + candidates.size() >= maxCount) {
          break;
        }
        if (resource.getTierLevel() == tierLevel
            && resource.getStatus() == TsFileResourceStatus.NORMAL
            && resource.isClosed()) {
          candidates.add(resource);
        }
      }
    }

    // Sort by file end time (oldest first) and migrate
    candidates.sort((a, b) -> Long.compare(a.getFileEndTime(), b.getFileEndTime()));

    for (TsFileResource resource : candidates) {
      if (migratedCount >= maxCount) {
        break;
      }

      if (resource.transformStatus(TsFileResourceStatus.MIGRATING)) {
        // Revert status - the task will set it again
        resource.transformStatus(TsFileResourceStatus.NORMAL);

        final TierMigrationTask task =
            new TierMigrationTask(resource, tierLevel + 1, resource.isSeq());
        task.run();
        migratedCount++;
      }
    }

    return migratedCount;
  }
}
