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

package py.dih.client.worker;

import py.app.context.AppContext;
import py.instance.InstanceStore;
import py.periodic.Worker;
import py.periodic.WorkerFactory;

/**
 * This factory is not thread safe. It is expected that the factory is a singleton object injected
 * by spring.
 */
public class CheckMasterHeartBeatWorkerFactory implements WorkerFactory {
  private static CheckMasterHeartBeatWorker worker = null;

  private InstanceStore instanceStore;
  private AppContext appContext;

  @Override
  public Worker createWorker() {
    if (worker == null) {
      worker = new CheckMasterHeartBeatWorker();
      worker.setAppContext(appContext);
      worker.setInstanceStore(instanceStore);
    }
    return worker;
  }

  public InstanceStore getInstanceStore() {
    return instanceStore;
  }

  public void setInstanceStore(InstanceStore instanceStore) {
    this.instanceStore = instanceStore;
  }

  public AppContext getAppContext() {
    return appContext;
  }

  public void setAppContext(AppContext appContext) {
    this.appContext = appContext;
  }
}
