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

package py.infocenter.client;

import py.client.ClientWrapperFactory;
import py.client.thrift.GenericThriftClientFactory;
import py.common.PyService;
import py.thrift.infocenter.service.InformationCenter;

/**
 * The factory generates an information center client wrapper.
 */
public class InformationCenterClientFactory extends
    ClientWrapperFactory<InformationCenter.Iface, InformationCenterClientWrapper> {
  public InformationCenterClientFactory() {
    init();
    genericClientFactory = GenericThriftClientFactory.create(InformationCenter.Iface.class);
  }

  public InformationCenterClientFactory(int minWorkThreadCount) {
    init();
    genericClientFactory = GenericThriftClientFactory
        .create(InformationCenter.Iface.class, minWorkThreadCount);
  }

  public InformationCenterClientFactory(int minWorkThreadCount, int connectionTimeoutMs) {
    init();
    genericClientFactory = GenericThriftClientFactory
        .create(InformationCenter.Iface.class, minWorkThreadCount)
        .withDefaultConnectionTimeout(connectionTimeoutMs);
  }

  public InformationCenterClientFactory(int minWorkThreadCount, int maxWorkThreadCount,
      int connectionTimeoutMs) {
    init();
    genericClientFactory = GenericThriftClientFactory
        .create(InformationCenter.Iface.class, minWorkThreadCount,
            maxWorkThreadCount).withDefaultConnectionTimeout(connectionTimeoutMs);
  }

  @Override
  protected void init() {
    this.setDelegatable(true);
    this.setClientClass(InformationCenter.Iface.class);
    this.setClientWrapperClass(InformationCenterClientWrapper.class);
    this.setInstanceName(PyService.INFOCENTER.getServiceName());
  }
}
