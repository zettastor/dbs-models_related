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

package py.volume;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * information about rebalance.
 */
public class VolumeRebalanceInfo {
  private static final Logger logger = LoggerFactory.getLogger(VolumeRebalanceInfo.class);

  private double rebalanceRatio = 1.0;        //rebalance progress
  private long rebalanceTotalTaskCount;       //total rebalance task count
  private long rebalanceRemainTaskCount;      //rebalance task count that not run over
  private long rebalanceVersion;              //rebalance Version(times, tell us why rebalance ratio
  // changes when volume environment changed)
  private boolean isRebalanceCalculating;     //is rebalance calculating

  public double getRebalanceRatio() {
    return rebalanceRatio;
  }

  public void setRebalanceRatio(double rebalanceRatio) {
    this.rebalanceRatio = rebalanceRatio;
  }

  public long getRebalanceTotalTaskCount() {
    return rebalanceTotalTaskCount;
  }

  public void setRebalanceTotalTaskCount(long rebalanceTotalTaskCount) {
    this.rebalanceTotalTaskCount = rebalanceTotalTaskCount;
  }

  public long getRebalanceRemainTaskCount() {
    return rebalanceRemainTaskCount;
  }

  public void setRebalanceRemainTaskCount(long rebalanceRemainTaskCount) {
    this.rebalanceRemainTaskCount = rebalanceRemainTaskCount;
  }

  public boolean isRebalanceCalculating() {
    return isRebalanceCalculating;
  }

  public void setRebalanceCalculating(boolean rebalanceCalculating) {
    isRebalanceCalculating = rebalanceCalculating;
  }

  public long getRebalanceVersion() {
    return rebalanceVersion;
  }

  public void setRebalanceVersion(long rebalanceVersion) {
    this.rebalanceVersion = rebalanceVersion;
  }

  public VolumeRebalanceInfo deepCopy(VolumeRebalanceInfo src) {
    if (src == null) {
      return null;
    }

    this.rebalanceRatio = src.getRebalanceRatio();
    this.rebalanceTotalTaskCount = src.getRebalanceTotalTaskCount();
    this.rebalanceRemainTaskCount = src.getRebalanceRemainTaskCount();
    this.rebalanceVersion = src.getRebalanceVersion();
    this.isRebalanceCalculating = src.isRebalanceCalculating();
    return this;
  }

  /**
   * calculate rebalance ratio.
   */
  public void calcRatio() {
    if (rebalanceTotalTaskCount == 0) {
      if (isRebalanceCalculating) {
        //means rebalance step is calculating
        rebalanceRatio = 0.0;
      } else {
        //means no rebalance doing
        rebalanceRatio = 1.0;
      }
    } else {
      //means has rebalance doing
      rebalanceRatio =
          (double) (rebalanceTotalTaskCount - rebalanceRemainTaskCount) / rebalanceTotalTaskCount;
    }

    if (rebalanceRatio < 0) {
      logger
          .error("remain rebalance task steps is bigger than total task steps, remain:{}, total:{}",
              rebalanceRemainTaskCount, rebalanceTotalTaskCount);
      rebalanceRatio = 0;
    }
  }

  @Override
  public String toString() {
    return "VolumeRebalanceInfo{"
        + "rebalanceRatio=" + rebalanceRatio
        + ", rebalanceTotalTaskCount=" + rebalanceTotalTaskCount
        + ", rebalanceRemainTaskCount=" + rebalanceRemainTaskCount
        + ", rebalanceVersion=" + rebalanceVersion
        + ", isRebalanceCalculating=" + isRebalanceCalculating
        + '}';
  }
}
