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
