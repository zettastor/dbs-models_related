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

import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.app.context.AppContext;
import py.common.PyService;
import py.common.struct.EndPoint;
import py.instance.Instance;
import py.instance.InstanceStatus;
import py.instance.InstanceStore;
import py.periodic.Worker;

public class CheckMasterHeartBeatWorker implements Worker {
  private static final Logger logger = LoggerFactory.getLogger(CheckMasterHeartBeatWorker.class);
  private AppContext appContext;
  private InstanceStore instanceStore;

  public AppContext getAppContext() {
    return appContext;
  }

  public void setAppContext(AppContext appContext) {
    this.appContext = appContext;
  }

  public void setInstanceStore(InstanceStore instanceStore) {
    this.instanceStore = instanceStore;
  }

  @Override
  public void doWork() throws Exception {
    if (appContext.getStatus() != InstanceStatus.HEALTHY) {
      logger.info("only the master do check master status");
      return;
    }

    Set<Instance> allDihInstances = instanceStore
        .getAll(PyService.DIH.getServiceName());
    EndPoint endPoint = appContext.getMainEndPoint();

    logger.info("do check the master work, the allDihInstances:{}, my endPoint:{}", allDihInstances,
        endPoint);
    for (Instance instance : allDihInstances) {
      EndPoint endPointDih = instance.getEndPoint();
      if (endPointDih.getHostName().equals(endPoint.getHostName())
          && instance.getStatus() != InstanceStatus.HEALTHY) {
        logger.warn(
            "in check server:{}, {}, find it is master, but the dih:{} is not good, kill my self "
                + "for change",
            appContext.getInstanceName(), endPoint, instance);
        System.exit(0);
      }
    }
  }
}
