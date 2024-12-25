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

package py.monitorserver.client;

import py.client.ClientWrapperFactory;
import py.client.thrift.GenericThriftClientFactory;
import py.common.PyService;
import py.thrift.monitorserver.service.MonitorServer;

/**
 * The factory generates a monitor server client wrapper.
 */
public class MonitorServerClientFactory extends
    ClientWrapperFactory<MonitorServer.Iface, MonitorServerClientWrapper> {
  public MonitorServerClientFactory() {
    init();
    genericClientFactory = GenericThriftClientFactory.create(MonitorServer.Iface.class);
  }

  public MonitorServerClientFactory(int minWorkThreadCount) {
    init();
    genericClientFactory = GenericThriftClientFactory
        .create(MonitorServer.Iface.class, minWorkThreadCount);
  }

  public MonitorServerClientFactory(int minWorkThreadCount, int connectionTimeoutMs) {
    init();
    genericClientFactory = GenericThriftClientFactory
        .create(MonitorServer.Iface.class, minWorkThreadCount)
        .withDefaultConnectionTimeout(connectionTimeoutMs);
  }

  public MonitorServerClientFactory(int minWorkThreadCount, int maxWorkThreadCount,
      int connectionTimeoutMs) {
    init();
    genericClientFactory = GenericThriftClientFactory
        .create(MonitorServer.Iface.class, minWorkThreadCount,
            maxWorkThreadCount).withDefaultConnectionTimeout(connectionTimeoutMs);
  }

  @Override
  public void init() {
    this.setDelegatable(true);
    this.setClientClass(MonitorServer.Iface.class);
    this.setClientWrapperClass(MonitorServerClientWrapper.class);
  }
}
