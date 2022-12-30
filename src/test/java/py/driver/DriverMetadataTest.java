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

import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.test.TestBase;
import py.volume.ClosedRangeSetParser;

/**
 * A class contains some tests for {@link DriverMetadata}.
 */
public class DriverMetadataTest extends TestBase {
  private static final Logger logger = LoggerFactory.getLogger(DriverMetadataTest.class);

  @Override
  public void init() throws Exception {
    super.init();
  }

  @Test
  public void testSetStatusWithFailure() throws Exception {
    DriverMetadata driverMetadata = new DriverMetadata();
    driverMetadata.setDriverStatus(DriverStatus.LAUNCHED);
    driverMetadata.setDriverStatusIfValid(DriverStatus.RECOVERING, DriverAction.START_SERVER);
    Assert.assertEquals(DriverStatus.UNKNOWN,
        driverMetadata.setDriverStatusIfValid(DriverStatus.REMOVING, DriverAction.START_SERVER));
    Assert.assertEquals(DriverStatus.RECOVERING, driverMetadata.getDriverStatus());
  }

  @Test
  public void testSaveToFileForAclList() throws Exception {
    DriverMetadata driverMetadata = new DriverMetadata();
    List<IscsiAccessRule> aclList = new ArrayList<>();
    IscsiAccessRule rule = new IscsiAccessRule("iqn.1994-05.com.redhat:225", "", "", "", "");
    IscsiAccessRule rule1 = new IscsiAccessRule("iqn.1994-05.com.redhat:226", "", "", "", "");
    aclList.add(rule);
    aclList.add(rule1);
    driverMetadata.setAclList(aclList);
    Path pathBak = Paths.get("/tmp/Acl_list0_bak");
    Path path = Paths.get("/tmp/Acl_list0");
    driverMetadata.saveToFileForAclList(path, pathBak);
    List<IscsiAccessRule> aclList1 = driverMetadata.buildFromFileForAclList(path);
    Assert.assertTrue(aclList1.get(0).getInitiatorName().equals("iqn.1994-05.com.redhat:225"));
    Assert.assertTrue(aclList1.get(1).getInitiatorName().equals("iqn.1994-05.com.redhat:226"));
  }

  @Test
  public void testCheck() {
    RangeSet<Integer> volumeLayoutRange = TreeRangeSet.create();

    volumeLayoutRange.add(Range.closed(0, 1));

    List<Range<Integer>> rangesToRemove = new ArrayList<Range<Integer>>();
    for (Range<Integer> range : volumeLayoutRange.asRanges()) {
      if (range.lowerBoundType() == BoundType.OPEN) {
        rangesToRemove.add(Range.open(range.lowerEndpoint(), range.lowerEndpoint() + 1));
      }
      if (range.upperBoundType() == BoundType.OPEN) {
        rangesToRemove.add(Range.open(range.upperEndpoint() - 1, range.upperEndpoint()));
      }
    }

    for (Range<Integer> range : rangesToRemove) {
      volumeLayoutRange.remove(range);
    }
    String volumeLayoutString = volumeLayoutRange.toString();
    logger.warn("get the :{}", volumeLayoutString);
    volumeLayoutRange = ClosedRangeSetParser.parseRange(volumeLayoutString);
    logger.warn("get the :{}", volumeLayoutRange);
  }

  @Test
  public void testDriverStatusPriority() {
    List<DriverStatus> driverStatusList = new ArrayList<>();
    for (int i = 1; i < 11; i++) {
      driverStatusList.add(DriverStatus.findByValue(i));
    }

    Collections.shuffle(driverStatusList);
    logger.warn("the list is :{}", driverStatusList);
    Collections.sort(driverStatusList, new Comparator<DriverStatus>() {
      @Override
      public int compare(DriverStatus o1, DriverStatus o2) {
        return o1.getPriorityForCsi() - o2.getPriorityForCsi();
      }
    });

    logger.warn("the list is :{}", driverStatusList);

  }

}
