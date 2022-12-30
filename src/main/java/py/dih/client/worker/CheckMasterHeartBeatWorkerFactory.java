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
