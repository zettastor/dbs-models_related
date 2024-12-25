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
