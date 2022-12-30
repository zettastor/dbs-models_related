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