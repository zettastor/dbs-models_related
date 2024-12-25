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

package py.app.healthcheck;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.periodic.WorkerFactory;
import py.periodic.impl.ExecutionOptionsReader;
import py.periodic.impl.PeriodicWorkExecutorImpl;

/**
 * check instance health periodically.
 */
public class CheckMasterHealthCheckerImpl implements HealthChecker {
  private static final Logger logger = LoggerFactory.getLogger(CheckMasterHealthCheckerImpl.class);

  // Setters
  private int checkingRate;
  // Internal variables
  private PeriodicWorkExecutorImpl executor;
  private WorkerFactory checkMasterHeartBeatWorkerFactory;

  public CheckMasterHealthCheckerImpl(int checkingRate,
      WorkerFactory checkMasterHeartBeatWorkerFactory) {
    super();
    this.checkingRate = checkingRate;
    this.checkMasterHeartBeatWorkerFactory = checkMasterHeartBeatWorkerFactory;
  }

  @Override
  public void startHealthCheck() throws Exception {
    logger.warn("begin the check master health checker");
    ExecutionOptionsReader optionReader = new ExecutionOptionsReader(1, 1, checkingRate, null);
    executor = new PeriodicWorkExecutorImpl(optionReader, checkMasterHeartBeatWorkerFactory,
        "master-health-checker");
    executor.start();
  }

  @Override
  public void stopHealthCheck() {
    // Stop the executor immediately. No meaning to wait
    executor.stopNow();
    if (checkMasterHeartBeatWorkerFactory != null) {
      checkMasterHeartBeatWorkerFactory = null;
    }
  }

}
