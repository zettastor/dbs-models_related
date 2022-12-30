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

package py.infocenter.common;

public class InfoCenterConstants {
  public static final Long volumeSize = 1073741824L;
  public static final Long segmentSize = 1073741824L;
  public static final String name = "_lost";
  public static int volumeToBeCreatedTimeout = 90; //default value is 90s
  public static int volumeBeCreatingTimeout = 1800; //default value is 1800s
  public static int segmentUnitReportTimeout = 90; //default value is 90s
  public static int timeOfdeadVolumeToRemove = 15552000; //the exist time of dead volume. After
  // the time, the volume will be removed in memory and DB

  public static int fixVolumeTimeoutSec = 600;

  public static long refreshPeriodTime = 29000;

  @Deprecated
  public static int maxRebalanceTaskCount = 5;

  @Deprecated
  public static int getMaxRebalanceTaskCount() {
    return maxRebalanceTaskCount;
  }

  @Deprecated
  public static void setMaxRebalanceTaskCount(int maxRebalanceTaskCount) {
    InfoCenterConstants.maxRebalanceTaskCount = maxRebalanceTaskCount;
  }

  public static int getTimeOfdeadVolumeToRemove() {
    return timeOfdeadVolumeToRemove;
  }

  public static void setTimeOfdeadVolumeToRemove(int timeOfdeadVolumeToRemove) {
    InfoCenterConstants.timeOfdeadVolumeToRemove = timeOfdeadVolumeToRemove;
  }

  public static int getSegmentUnitReportTimeout() {
    return segmentUnitReportTimeout;
  }

  public static void setSegmentUnitReportTimeout(int segmentUnitReportTimeout) {
    InfoCenterConstants.segmentUnitReportTimeout = segmentUnitReportTimeout;
  }

  public static int getVolumeToBeCreatedTimeout() {
    return volumeToBeCreatedTimeout;
  }

  public static void setVolumeToBeCreatedTimeout(int volumeToBeCreatedTimeout) {
    InfoCenterConstants.volumeToBeCreatedTimeout = volumeToBeCreatedTimeout;
  }

  public static int getVolumeBeCreatingTimeout() {
    return volumeBeCreatingTimeout;
  }

  public static void setVolumeBeCreatingTimeout(int volumeBeCreatingTimeout) {
    InfoCenterConstants.volumeBeCreatingTimeout = volumeBeCreatingTimeout;
  }

  public static long getRefreshPeriodTime() {
    return refreshPeriodTime;
  }

  public static void setRefreshPeriodTime(long refreshPeriodTime) {
    InfoCenterConstants.refreshPeriodTime = refreshPeriodTime;
  }

  public static int getFixVolumeTimeoutSec() {
    return fixVolumeTimeoutSec;
  }

  public static void setFixVolumeTimeoutSec(int fixVolumeTimeoutSec) {
    InfoCenterConstants.fixVolumeTimeoutSec = fixVolumeTimeoutSec;
  }

}
