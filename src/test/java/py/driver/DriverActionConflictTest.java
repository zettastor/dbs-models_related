/*
 * Copyright (c) 2022-2022. PengYunNetWork
 *
 * This program is free software: you can use, redistribute, and/or modify it
 * under the terms of the GNU Affero General Public License, version 3 or later ("AGPL"),
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 *  without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 *
 *  You should have received a copy of the GNU Affero General Public License along with
 *  this program. If not, see <http://www.gnu.org/licenses/>.
 */

package py.driver;

import org.junit.Assert;
import org.junit.Test;
import py.test.TestBase;

/**
 * Test different driver action conflict or not.
 */
public class DriverActionConflictTest extends TestBase {
  @Override
  public void init() throws Exception {
    super.init();
  }

  @Test
  public void testDriverActionConflict() {
    Assert.assertTrue(DriverAction.CHECK_MIGRATING.isConflictWith(DriverAction.CHANGE_VOLUME));
    Assert.assertTrue(DriverAction.CHECK_MIGRATING.isConflictWith(DriverAction.CHECK_MIGRATING));
    Assert.assertFalse(DriverAction.CHANGE_VOLUME.isConflictWith(DriverAction.CHECK_SERVER));
  }
}
