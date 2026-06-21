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

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class TierMigrationManagerTest {

  @Test
  public void testGetInstance() {
    TierManager manager1 = TierManager.getInstance();
    TierManager manager2 = TierManager.getInstance();
    Assert.assertSame("TierManager should be singleton", manager1, manager2);
  }

  @Test
  public void testGetMaxTierLevel() {
    int maxTierLevel = TierManager.getInstance().getMaxTierLevel();
    Assert.assertTrue("Max tier level should be >= 0", maxTierLevel >= 0);
  }

  @Test
  public void testGetTiersNum() {
    int tiersNum = TierManager.getInstance().getTiersNum();
    Assert.assertTrue("Tiers num should be >= 1", tiersNum >= 1);
  }

  @Test
  public void testGetTierDiskUsage() {
    double usage = TierManager.getInstance().getTierDiskUsage(0);
    Assert.assertTrue("Disk usage should be >= 0", usage >= 0.0);
    Assert.assertTrue("Disk usage should be <= 1.0", usage <= 1.0);
  }

  @Test
  public void testGetTierDiskUsageInvalidTier() {
    double usage = TierManager.getInstance().getTierDiskUsage(999);
    Assert.assertEquals("Invalid tier should return 0.0", 0.0, usage, 0.001);
  }
}
