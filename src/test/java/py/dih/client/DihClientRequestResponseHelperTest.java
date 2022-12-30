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

package py.dih.client;

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import py.common.struct.EndPoint;
import py.instance.Group;
import py.instance.Instance;
import py.instance.InstanceId;
import py.instance.InstanceStatus;
import py.instance.Location;
import py.instance.PortType;
import py.test.TestBase;
import py.thrift.distributedinstancehub.service.InstanceThrift;

public class DihClientRequestResponseHelperTest extends TestBase {
  @Test
  public void testBuildEndPoints() throws Exception {
    InstanceId id1 = new InstanceId(1L);
    InstanceId id2 = new InstanceId(2L);

    Instance instance = new Instance(id1, new Group(1), new Location("c", "1"), "aaaa",
        InstanceStatus.HEALTHY);
    instance.putEndPointByServiceName(PortType.CONTROL, new EndPoint("111", 1));
    testEquals(instance);

    instance = new Instance(id2, new Group(1), "aaaa", InstanceStatus.HEALTHY);
    instance.putEndPointByServiceName(PortType.CONTROL, new EndPoint("111", 2));
    testEquals(instance);

    instance = new Instance(id2, "name", InstanceStatus.HEALTHY, new EndPoint("1111", 3));
    testEquals(instance);
  }

  private void testEquals(Instance instance) throws Exception {
    InstanceThrift instanceThrift = DihClientRequestResponseHelper
        .buildThriftInstanceFrom(instance);
    Instance builtInstance = DihClientRequestResponseHelper.buildInstanceFrom(instanceThrift);
    assertEquals(instance, builtInstance);
  }
}