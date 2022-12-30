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
