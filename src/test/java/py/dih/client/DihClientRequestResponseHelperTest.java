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