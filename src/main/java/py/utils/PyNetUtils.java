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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.net.util.SubnetUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PyNetUtils {
  private static final Logger logger = LoggerFactory.getLogger(PyNetUtils.class);

  /**
   * Parse all hosts in the given host range string.
   *
   * <p>The format of host range string is like "10.0.1.1:10.0.1.10,10.0.1.23"; In the format, we
   * use ":" to represent a continuous range, and use "," to split discrete range.
   */
  public static List<String> getAllHostsInRange(String hostRange, String subNetwork) {
    if (hostRange == null || hostRange.isEmpty()) {
      return new ArrayList<String>();
    }

    List<String> serviceHostSubRanges = Arrays.asList(hostRange.split(","));

    SubnetUtils subnetUtils = new SubnetUtils(subNetwork);
    List<String> allHostsInSubnet = Arrays.asList(subnetUtils.getInfo().getAllAddresses());

    List<String> serviceDeploymentHosts = new ArrayList<String>();
    for (String subRange : serviceHostSubRanges) {
      List<String> rangeEnds = Arrays.asList(subRange.split(":"));
      for (String end : rangeEnds) {
        if (!allHostsInSubnet.contains(end)) {
          logger.error("Unable to initialize deployment configuration");
          throw new RuntimeException("Unable to initialize deployment configuration");
        }
      }

      switch (rangeEnds.size()) {
        case 1:
          String serviceDeploymentHost = rangeEnds.get(0);
          serviceDeploymentHosts.add(serviceDeploymentHost);
          continue;
        case 2:
          String oneEnd = rangeEnds.get(0);
          String theOtherEnd = rangeEnds.get(1);

          int indexOfOneEnd = allHostsInSubnet.indexOf(oneEnd);
          int indexOfTheOtherEnd = allHostsInSubnet.indexOf(theOtherEnd);

          for (int i = Math.min(indexOfOneEnd, indexOfTheOtherEnd);
              i <= Math.max(indexOfTheOtherEnd,
                  indexOfOneEnd); i++) {
            serviceDeploymentHosts.add(allHostsInSubnet.get(i));
          }
          continue;
        default:
          serviceDeploymentHosts.clear();
          throw new RuntimeException("Unable to initialize deployment configuration");
      }
    }

    return serviceDeploymentHosts;
  }
}
