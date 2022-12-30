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

import java.util.List;
import net.sf.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.io.qos.IoLimitation;
import py.io.qos.RebalanceAbsoluteTime;
import py.io.qos.RebalanceRelativeTime;

public class RebalanceRule {
  public static final long MIN_WAIT_TIME = 60;   //unit:s
  private static final Logger logger = LoggerFactory.getLogger(IoLimitation.class);
  private long ruleId;
  private String ruleName;

  private List<RebalanceAbsoluteTime> absoluteTimeList; // required
  private RebalanceRelativeTime relativeTime; // required

  public long getRuleId() {
    return ruleId;
  }

  public void setRuleId(long ruleId) {
    this.ruleId = ruleId;
  }

  public String getRuleName() {
    return ruleName;
  }

  public void setRuleName(String ruleName) {
    this.ruleName = ruleName;
  }

  public List<RebalanceAbsoluteTime> getAbsoluteTimeList() {
    return absoluteTimeList;
  }

  public void setAbsoluteTimeList(List<RebalanceAbsoluteTime> absoluteTimeList) {
    this.absoluteTimeList = absoluteTimeList;
  }

  public RebalanceRelativeTime getRelativeTime() {
    return relativeTime;
  }

  public void setRelativeTime(RebalanceRelativeTime relativeTime) {
    this.relativeTime = relativeTime;
  }

  public RebalanceRuleInformation toRebalanceRuleInformation() {
    RebalanceRuleInformation rule = new RebalanceRuleInformation();
    rule.setRuleId(ruleId);
    rule.setRuleName(ruleName);

    //relative time
    if (relativeTime != null) {
      long waitTime = relativeTime.getWaitTime();
      if (waitTime < MIN_WAIT_TIME) {
        waitTime = MIN_WAIT_TIME;
      }
      rule.setWaitTime(waitTime);
    }

    //absolute time
    if (absoluteTimeList != null) {
      JSONArray absoluteTimeJsonArray = JSONArray.fromObject(absoluteTimeList);
      rule.setAbsoluteTimeJson(absoluteTimeJsonArray.toString());
    }

    return rule;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    RebalanceRule that = (RebalanceRule) o;

    if (ruleId != that.ruleId) {
      return false;
    }
    if (ruleName != null ? !ruleName.equals(that.ruleName) : that.ruleName != null) {
      return false;
    }
    if (absoluteTimeList != null ? !absoluteTimeList.equals(that.absoluteTimeList)
        : that.absoluteTimeList != null) {
      return false;
    }
    return relativeTime != null ? relativeTime.equals(that.relativeTime)
        : that.relativeTime == null;
  }

  @Override
  public int hashCode() {
    int result = (int) (ruleId ^ (ruleId >>> 32));
    result = 31 * result + (ruleName != null ? ruleName.hashCode() : 0);
    result = 31 * result + (absoluteTimeList != null ? absoluteTimeList.hashCode() : 0);
    result = 31 * result + (relativeTime != null ? relativeTime.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "RebalanceRule{"
        + "ruleId=" + ruleId
        + ", ruleName='" + ruleName + '\''
        + ", absoluteTimeList=" + absoluteTimeList
        + ", relativeTime=" + relativeTime
        + '}';
  }
}
