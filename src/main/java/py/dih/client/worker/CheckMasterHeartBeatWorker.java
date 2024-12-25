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
