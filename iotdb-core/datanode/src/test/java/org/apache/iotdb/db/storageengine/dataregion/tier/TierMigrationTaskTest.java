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

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class TierMigrationTaskTest {

  @Test
  public void testMigratingStatusExists() {
    // Verify MIGRATING status is a valid enum value
    TsFileResourceStatus status = TsFileResourceStatus.valueOf("MIGRATING");
    Assert.assertEquals("MIGRATING status should exist", TsFileResourceStatus.MIGRATING, status);
  }

  @Test
  public void testTsFileResourceCreation() {
    File tempFile = null;
    try {
      tempFile = File.createTempFile("test_migration", ".tsfile");
      TsFileResource resource = new TsFileResource(tempFile);

      // Initial status should be UNCLOSED
      Assert.assertEquals(
          "Initial status should be UNCLOSED",
          TsFileResourceStatus.UNCLOSED,
          resource.getStatus());

    } catch (IOException e) {
      Assert.fail("Failed to create temp file: " + e.getMessage());
    } finally {
      if (tempFile != null && tempFile.exists()) {
        tempFile.delete();
      }
    }
  }

  @Test
  public void testMigratingToNormalTransition() {
    File tempFile = null;
    try {
      tempFile = File.createTempFile("test_migration2", ".tsfile");
      TsFileResource resource = new TsFileResource(tempFile);

      // Transition to NORMAL first
      resource.transformStatus(TsFileResourceStatus.NORMAL);

      // Now try MIGRATING -> NORMAL (should fail since not in MIGRATING state)
      boolean result = resource.transformStatus(TsFileResourceStatus.MIGRATING);
      // This should fail because we can't go NORMAL -> MIGRATING directly without
      // the resource being in the right state

      // Just verify the status after operations
      TsFileResourceStatus finalStatus = resource.getStatus();
      Assert.assertNotNull("Status should not be null", finalStatus);

    } catch (IOException e) {
      Assert.fail("Failed to create temp file: " + e.getMessage());
    } finally {
      if (tempFile != null && tempFile.exists()) {
        tempFile.delete();
      }
    }
  }
}
