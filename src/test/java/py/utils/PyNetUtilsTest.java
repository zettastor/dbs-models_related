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

package py.utils;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;
import py.test.TestBase;

public class PyNetUtilsTest extends TestBase {
  @Test
  public void testGettingAllHostsInRange() {
    String subNetwork = "10.0.1.1/24";

    String validHostRange1 = "10.0.1.1";
    String validHostRange2 = "10.0.1.1:10.0.1.3";
    String validHostRange3 = "10.0.1.1:10.0.1.3,10.0.1.5";
    String validHostRange4 = "10.0.1.1:10.0.1.3,10.0.1.5:10.0.1.6";
    String validHostRange5 = "10.0.1.3:10.0.1.1";

    final String[] hostsInValidHostRange1 = {"10.0.1.1"};
    final String[] hostsInValidHostRange2 = {"10.0.1.1", "10.0.1.2", "10.0.1.3"};
    final String[] hostsInValidHostRange3 = {"10.0.1.1", "10.0.1.2", "10.0.1.3", "10.0.1.5"};

    List<String> hostListInValidHostRange1 = PyNetUtils
        .getAllHostsInRange(validHostRange1, subNetwork);
    List<String> hostListInValidHostRange2 = PyNetUtils
        .getAllHostsInRange(validHostRange2, subNetwork);
    List<String> hostListInValidHostRange3 = PyNetUtils
        .getAllHostsInRange(validHostRange3, subNetwork);
    List<String> hostListInValidHostRange4 = PyNetUtils
        .getAllHostsInRange(validHostRange4, subNetwork);
    List<String> hostListInValidHostRange5 = PyNetUtils
        .getAllHostsInRange(validHostRange5, subNetwork);

    Collections.sort(hostListInValidHostRange1);
    Collections.sort(hostListInValidHostRange2);
    Collections.sort(hostListInValidHostRange3);
    Collections.sort(hostListInValidHostRange4);
    Collections.sort(hostListInValidHostRange5);

    Assert.assertTrue(hostListInValidHostRange1.equals(Arrays.asList(hostsInValidHostRange1)));
    Assert.assertTrue(hostListInValidHostRange2.equals(Arrays.asList(hostsInValidHostRange2)));
    Assert.assertTrue(hostListInValidHostRange3.equals(Arrays.asList(hostsInValidHostRange3)));
    String[] hostsInValidHostRange4 = {"10.0.1.1", "10.0.1.2", "10.0.1.3", "10.0.1.5", "10.0.1.6"};
    Assert.assertTrue(hostListInValidHostRange4.equals(Arrays.asList(hostsInValidHostRange4)));
    Assert.assertTrue(hostListInValidHostRange5.equals(Arrays.asList(hostsInValidHostRange2)));

    String invalidHostRange1 = "10.1.1.1";
    String invalidHostRange2 = "10.0.1.1::10.0.1.3";
    String invalidHostRange3 = "10.0.1.1:?10.0.1.3";

    boolean exceptionCached = false;
    try {
      PyNetUtils.getAllHostsInRange(invalidHostRange1, subNetwork);
    } catch (Exception e) {
      exceptionCached = true;
    }

    Assert.assertTrue(exceptionCached);

    exceptionCached = false;

    try {
      PyNetUtils.getAllHostsInRange(invalidHostRange2, subNetwork);
    } catch (Exception e) {
      exceptionCached = true;
    }

    Assert.assertTrue(exceptionCached);

    exceptionCached = false;

    try {
      PyNetUtils.getAllHostsInRange(invalidHostRange3, subNetwork);
    } catch (Exception e) {
      exceptionCached = true;
    }

    Assert.assertTrue(exceptionCached);
  }

}