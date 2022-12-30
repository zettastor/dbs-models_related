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

package py.monitorcenter.client;

import py.client.ClientWrapperFactory;
import py.client.thrift.GenericThriftClientFactory;
import py.common.PyService;
import py.thrift.systemmonitor.service.MonitorCenter;

/**
 * use this class to create a monitor centre client wrapper.
 *
 */
public class MonitorCenterClientFactory extends
    ClientWrapperFactory<MonitorCenter.Iface, MonitorCenterClientWrapper> {
  public MonitorCenterClientFactory() {
    init();
    genericClientFactory = GenericThriftClientFactory.create(MonitorCenter.Iface.class);
  }

  public MonitorCenterClientFactory(int minWorkThreadCount) {
    init();
    genericClientFactory = GenericThriftClientFactory
        .create(MonitorCenter.Iface.class, minWorkThreadCount);
  }

  public MonitorCenterClientFactory(int minWorkThreadCount, int connectionTimeoutMs) {
    init();
    genericClientFactory = GenericThriftClientFactory
        .create(MonitorCenter.Iface.class, minWorkThreadCount)
        .withDefaultConnectionTimeout(connectionTimeoutMs);
  }

  public MonitorCenterClientFactory(int minWorkThreadCount, int maxWorkThreadCount,
      int connectionTimeoutMs) {
    init();
    genericClientFactory = GenericThriftClientFactory
        .create(MonitorCenter.Iface.class, minWorkThreadCount,
            maxWorkThreadCount).withDefaultConnectionTimeout(connectionTimeoutMs);
  }

  @Override
  public void init() {
    this.setDelegatable(true);
    this.setClientClass(MonitorCenter.Iface.class);
    this.setClientWrapperClass(MonitorCenterClientWrapper.class);
  }
}
