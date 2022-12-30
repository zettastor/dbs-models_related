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

package py.icshare.qos;

import py.io.qos.MigrationRuleStatus;
import py.io.qos.MigrationStrategy;

public class MigrationRuleInformation {
  private long ruleId;

  private String migrationRuleName;

  private long maxMigrationSpeed;

  private String migrationStrategy;

  private String status;

  private String checkSecondaryInactiveThresholdMode;
  private long startTime;
  private long endTime;
  private long waitTime;
  private boolean ignoreMissPagesAndLogs;
  private boolean builtInRule;

  public MigrationRuleInformation() {
  }

  public MigrationRuleInformation(long ruleId, String migrationSpeedName, long maxMigrationSpeed,
      String status) {
    this.ruleId = ruleId;
    this.migrationRuleName = migrationSpeedName;
    this.maxMigrationSpeed = maxMigrationSpeed;
    this.status = status;
  }

  public long getRuleId() {
    return ruleId;
  }

  public void setRuleId(long ruleId) {
    this.ruleId = ruleId;
  }

  public String getMigrationRuleName() {
    return migrationRuleName;
  }

  public void setMigrationRuleName(String migrationRuleName) {
    this.migrationRuleName = migrationRuleName;
  }

  public long getMaxMigrationSpeed() {
    return maxMigrationSpeed;
  }

  public void setMaxMigrationSpeed(long maxMigrationSpeed) {
    this.maxMigrationSpeed = maxMigrationSpeed;
  }

  public String getStatus() {
    return status;
  }

  public void setStatus(String status) {
    this.status = status;
  }

  public String getCheckSecondaryInactiveThresholdMode() {
    return checkSecondaryInactiveThresholdMode;
  }

  public void setCheckSecondaryInactiveThresholdMode(String checkSecondaryInactiveThresholdMode) {
    this.checkSecondaryInactiveThresholdMode = checkSecondaryInactiveThresholdMode;
  }

  public long getStartTime() {
    return startTime;
  }

  public void setStartTime(long startTime) {
    this.startTime = startTime;
  }

  public long getEndTime() {
    return endTime;
  }

  public void setEndTime(long endTime) {
    this.endTime = endTime;
  }

  public long getWaitTime() {
    return waitTime;
  }

  public void setWaitTime(long waitTime) {
    this.waitTime = waitTime;
  }

  public boolean getIgnoreMissPagesAndLogs() {
    return ignoreMissPagesAndLogs;
  }

  public void setIgnoreMissPagesAndLogs(boolean ignoreMissPagesAndLogs) {
    this.ignoreMissPagesAndLogs = ignoreMissPagesAndLogs;
  }

  public String getMigrationStrategy() {
    return migrationStrategy;
  }

  public void setMigrationStrategy(String migrationStrategy) {
    this.migrationStrategy = migrationStrategy;
  }

  public boolean isBuiltInRule() {
    return builtInRule;
  }

  public void setBuiltInRule(boolean builtInRule) {
    this.builtInRule = builtInRule;
  }

  public MigrationRule toMigrationRule() {
    MigrationRule migrationRule = new MigrationRule();
    migrationRule.setRuleId(ruleId);
    migrationRule.setMigrationRuleName(migrationRuleName);
    migrationRule.setMigrationStrategy(MigrationStrategy.valueOf(migrationStrategy));
    migrationRule.setMaxMigrationSpeed(maxMigrationSpeed);
    migrationRule.setStatus(MigrationRuleStatus.valueOf(status));
    migrationRule.setCheckSecondaryInactiveThresholdMode(
        CheckSecondaryInactiveThresholdMode.valueOf(checkSecondaryInactiveThresholdMode));
    migrationRule.setStartTime(startTime);
    migrationRule.setEndTime(endTime);
    migrationRule.setWaitTime(waitTime);
    migrationRule.setIgnoreMissPagesAndLogs(ignoreMissPagesAndLogs);
    migrationRule.setBuiltInRule(builtInRule);
    return migrationRule;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof MigrationRuleInformation)) {
      return false;
    }

    MigrationRuleInformation that = (MigrationRuleInformation) o;

    if (ruleId != that.ruleId) {
      return false;
    }
    if (maxMigrationSpeed != that.maxMigrationSpeed) {
      return false;
    }
    if (startTime != that.startTime) {
      return false;
    }
    if (endTime != that.endTime) {
      return false;
    }
    if (waitTime != that.waitTime) {
      return false;
    }
    if (ignoreMissPagesAndLogs != that.ignoreMissPagesAndLogs) {
      return false;
    }
    if (migrationRuleName != null
        ? !migrationRuleName.equals(that.migrationRuleName) : that.migrationRuleName != null) {
      return false;
    }
    if (migrationStrategy != null
        ? !migrationStrategy.equals(that.migrationStrategy) : that.migrationStrategy != null) {
      return false;
    }
    if (status != null ? !status.equals(that.status) : that.status != null) {
      return false;
    }
    if (builtInRule != that.builtInRule) {
      return false;
    }
    return checkSecondaryInactiveThresholdMode != null
        ? checkSecondaryInactiveThresholdMode.equals(that.checkSecondaryInactiveThresholdMode)
        : that.checkSecondaryInactiveThresholdMode == null;
  }

  @Override
  public int hashCode() {
    int result = (int) (ruleId ^ (ruleId >>> 32));
    result = 31 * result + (migrationRuleName != null ? migrationRuleName.hashCode() : 0);
    result = 31 * result + (int) (maxMigrationSpeed ^ (maxMigrationSpeed >>> 32));
    result = 31 * result + (migrationStrategy != null ? migrationStrategy.hashCode() : 0);
    result = 31 * result + (status != null ? status.hashCode() : 0);
    result = 31 * result + (checkSecondaryInactiveThresholdMode != null
        ? checkSecondaryInactiveThresholdMode.hashCode() : 0);
    result = 31 * result + (int) (startTime ^ (startTime >>> 32));
    result = 31 * result + (int) (endTime ^ (endTime >>> 32));
    result = 31 * result + (int) (waitTime ^ (waitTime >>> 32));
    result = 31 * result + (ignoreMissPagesAndLogs ? 1 : 0);
    result = 31 * result + (builtInRule ? 1 : 0);
    return result;
  }

  @Override
  public String toString() {
    return "MigrationRuleInformation{" + "ruleId=" + ruleId + ", migrationRuleName='"
        + migrationRuleName + '\''
        + ", maxMigrationSpeed=" + maxMigrationSpeed + ", migrationStrategy='" + migrationStrategy
        + '\''
        + ", status='" + status + '\'' + ", checkSecondaryInactiveThresholdMode='"
        + checkSecondaryInactiveThresholdMode + '\'' + ", startTime=" + startTime + ", endTime="
        + endTime
        + ", waitTime=" + waitTime + ", ignoreMissPagesAndLogs=" + ignoreMissPagesAndLogs
        + ", ignoreMissPagesAndLogs=" + ignoreMissPagesAndLogs + '}';
  }
}
