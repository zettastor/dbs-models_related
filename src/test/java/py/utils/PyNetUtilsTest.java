/**
* Copyright (C) 2013-2024 Nanjing Pengyun Network Technology Co., Ltd.
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
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