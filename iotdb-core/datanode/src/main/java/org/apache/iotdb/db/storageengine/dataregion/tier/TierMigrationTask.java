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

import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.db.storageengine.rescon.disk.TierManager;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

/**
 * Task to migrate a single TsFile from one storage tier to the next. Migration moves the TsFile
 * and its associated mod file to the target tier's directory, then updates the resource's tier
 * level.
 */
public class TierMigrationTask implements Runnable {
  private static final Logger LOGGER = LoggerFactory.getLogger(TierMigrationTask.class);

  private final TsFileResource resource;
  private final int targetTierLevel;
  private final boolean sequence;
  private boolean successful = false;
  private long ioBytes = 0;

  public TierMigrationTask(TsFileResource resource, int targetTierLevel, boolean sequence) {
    this.resource = resource;
    this.targetTierLevel = targetTierLevel;
    this.sequence = sequence;
  }

  public boolean isSuccessful() {
    return successful;
  }

  public long getIoBytes() {
    return ioBytes;
  }

  @Override
  public void run() {
    final File sourceFile = resource.getTsFile();
    final String sourcePath = sourceFile.getAbsolutePath();

    LOGGER.info(
        "TierMigration: Starting migration of {} from tier {} to tier {}",
        sourceFile.getName(),
        resource.getTierLevel(),
        targetTierLevel);

    // Wait for any active readers to finish before migration
    final long waitStart = System.currentTimeMillis();
    final long maxWaitMs = 30000; // 30 seconds max wait
    while (resource.getStatus() == TsFileResourceStatus.NORMAL
        && (System.currentTimeMillis() - waitStart) < maxWaitMs) {
      // Check if file is being read by pipe extractor or query
      // If status changes to MIGRATING or COMPACTING, skip wait
      if (resource.getStatus() != TsFileResourceStatus.NORMAL) {
        LOGGER.info(
            "TierMigration: Status changed for {}, skipping migration",
            sourceFile.getName());
        return;
      }
      try {
        Thread.sleep(1000); // Wait 1 second before retry
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
    }

    // Set status to MIGRATING
    if (!resource.transformStatus(TsFileResourceStatus.MIGRATING)) {
      LOGGER.warn(
          "TierMigration: Failed to set MIGRATING status for {}, skipping",
          sourceFile.getName());
      return;
    }

    try {
      // Get target directory
      final String targetDir;
      try {
        targetDir = TierManager.getInstance().getNextFolderForTsFile(targetTierLevel, sequence);
      } catch (Exception e) {
        LOGGER.warn(
            "TierMigration: Failed to get target directory for tier {}, skipping migration",
            targetTierLevel);
        resource.transformStatus(TsFileResourceStatus.NORMAL);
        return;
      }
      final File targetFile = new File(targetDir, sourceFile.getName());

      // Check if target file already exists
      if (targetFile.exists()) {
        LOGGER.warn(
            "TierMigration: Target file {} already exists, skipping migration",
            targetFile.getAbsolutePath());
        resource.transformStatus(TsFileResourceStatus.NORMAL);
        return;
      }

      // Move TsFile
      FileUtils.moveFile(sourceFile, targetFile);
      LOGGER.info("TierMigration: Moved TsFile {} to {}", sourceFile.getName(), targetDir);

      // Move mod file if exists
      final File sourceModFile = new File(sourcePath + ".mods");
      if (sourceModFile.exists()) {
        final File targetModFile = new File(targetDir, sourceFile.getName() + ".mods");
        FileUtils.moveFile(sourceModFile, targetModFile);
        LOGGER.info("TierMigration: Moved mod file {} to {}", sourceModFile.getName(), targetDir);
      }

      // Update resource file reference
      resource.setFile(targetFile);

      // Update tier level
      resource.increaseTierLevel();

      // Restore status to NORMAL
      resource.transformStatus(TsFileResourceStatus.NORMAL);

      // Record success and IO bytes
      successful = true;
      ioBytes = targetFile.length();

      LOGGER.info(
          "TierMigration: Successfully migrated {} to tier {}",
          sourceFile.getName(),
          targetTierLevel);

    } catch (IOException e) {
      LOGGER.error(
          "TierMigration: Failed to migrate {} from tier {} to tier {}",
          sourceFile.getName(),
          resource.getTierLevel(),
          targetTierLevel,
          e);

      // Rollback: try to move file back if it was moved
      try {
        final String targetDirPath =
            TierManager.getInstance().getNextFolderForTsFile(targetTierLevel, sequence);
        final File targetFile = new File(targetDirPath, sourceFile.getName());
        if (targetFile.exists() && !sourceFile.exists()) {
          FileUtils.moveFile(targetFile, sourceFile);
          LOGGER.info("TierMigration: Rolled back migration for {}", sourceFile.getName());
        }
      } catch (Exception rollbackException) {
        LOGGER.error(
            "TierMigration: Failed to rollback migration for {}",
            sourceFile.getName(),
            rollbackException);
      }

      // Restore status to NORMAL
      resource.transformStatus(TsFileResourceStatus.NORMAL);
    }
  }
}
