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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import net.sf.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.io.qos.IoLimitationInformation;
import py.io.qos.RebalanceAbsoluteTime;
import py.io.qos.RebalanceRelativeTime;

public class RebalanceRuleInformation {
  private static final Logger logger = LoggerFactory.getLogger(IoLimitationInformation.class);

  private long ruleId;
  private String ruleName;
  private String poolIds;

  private String absoluteTimeJson;            //absolute time
  private long waitTime;                      //relative time

  public static List<Long> poolIdStr2PoolIdList(String poolIdStr) {
    List<Long> poolIdList = new ArrayList<>();
    if (poolIdStr == null || poolIdStr.isEmpty()) {
      return poolIdList;
    }

    String[] poolIdArray = poolIdStr.split(",");
    for (String poolIdTemp : poolIdArray) {
      if (poolIdTemp == null || poolIdTemp.isEmpty()) {
        continue;
      }
      long poolId;
      try {
        poolId = Long.valueOf(poolIdTemp);
      } catch (Exception e) {
        logger.error("caught a exception when pool id string 2 pool id list,", e);
        continue;
      }
      poolIdList.add(poolId);
    }

    return poolIdList;
  }

  public static String pooIdList2PoolIdStr(Collection<Long> poolIdList) {
    StringBuilder poolIdStr = new StringBuilder(",");
    for (long poolId : poolIdList) {
      poolIdStr.append(poolId).append(",");
    }
    if (poolIdList.isEmpty()) {
      poolIdStr.append(",");
    }
    return poolIdStr.toString();
  }

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

  public String getAbsoluteTimeJson() {
    return absoluteTimeJson;
  }

  public void setAbsoluteTimeJson(String absoluteTimeJson) {
    this.absoluteTimeJson = absoluteTimeJson;
  }

  public String getPoolIds() {
    return poolIds;
  }

  public void setPoolIds(String poolIds) {
    this.poolIds = poolIds;
  }

  public long getWaitTime() {
    return waitTime;
  }

  public void setWaitTime(long waitTime) {
    this.waitTime = waitTime;
  }

  public RebalanceRule toRebalanceRule() {
    RebalanceRule rule = new RebalanceRule();
    rule.setRuleId(ruleId);
    rule.setRuleName(ruleName);

    //relative time
    RebalanceRelativeTime relativeTime = new RebalanceRelativeTime();
    if (waitTime < RebalanceRule.MIN_WAIT_TIME) {
      waitTime = RebalanceRule.MIN_WAIT_TIME;
    }
    relativeTime.setWaitTime(waitTime);
    rule.setRelativeTime(relativeTime);

    //absolute time
    rule.setAbsoluteTimeList(jsonStr2AbsoluteTime(absoluteTimeJson));

    return rule;
  }

  public List<RebalanceAbsoluteTime> jsonStr2AbsoluteTime(String jsonArrayStr) {
    logger.debug("json string to RebalanceAbsoluteTime: {}", jsonArrayStr);
    List<RebalanceAbsoluteTime> rebalanceAbsoluteTimeList = new ArrayList<>();
    if (jsonArrayStr == null || jsonArrayStr.isEmpty()) {
      return rebalanceAbsoluteTimeList;
    }
    try {
      JSONArray jsonArray = JSONArray.fromObject(jsonArrayStr);
      logger.debug("RebalanceAbsoluteTime: json array:{}", jsonArray);
      Iterator<Object> iterator = jsonArray.iterator();
      while (iterator.hasNext()) {
        String json = iterator.next().toString();
        RebalanceAbsoluteTime absoluteTime = RebalanceAbsoluteTime.fromJson(json);
        if (absoluteTime == null) {
          continue;
        }
        logger.debug("json:{} to absolute time:{}", json, absoluteTime);
        rebalanceAbsoluteTimeList.add(absoluteTime);
      }
    } catch (Exception e) {
      logger.error("cannot get TimeZone from db set it to empty", e);
    }
    return rebalanceAbsoluteTimeList;
  }

  public void copy(RebalanceRuleInformation src) {
    if (null == src) {
      return;
    }
    if (this == src) {
      return;
    }

    ruleId = src.getRuleId();
    ruleName = src.getRuleName();
    poolIds = src.getPoolIds();
    absoluteTimeJson = src.getAbsoluteTimeJson();
    waitTime = src.getWaitTime();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    RebalanceRuleInformation that = (RebalanceRuleInformation) o;

    if (ruleId != that.ruleId) {
      return false;
    }
    if (waitTime != that.waitTime) {
      return false;
    }
    if (ruleName != null ? !ruleName.equals(that.ruleName) : that.ruleName != null) {
      return false;
    }
    if (poolIds != null ? !poolIds.equals(that.poolIds) : that.poolIds != null) {
      return false;
    }
    return absoluteTimeJson != null ? absoluteTimeJson.equals(that.absoluteTimeJson)
        : that.absoluteTimeJson == null;
  }

  @Override
  public int hashCode() {
    int result = (int) (ruleId ^ (ruleId >>> 32));
    result = 31 * result + (ruleName != null ? ruleName.hashCode() : 0);
    result = 31 * result + (poolIds != null ? poolIds.hashCode() : 0);
    result = 31 * result + (absoluteTimeJson != null ? absoluteTimeJson.hashCode() : 0);
    result = 31 * result + (int) (waitTime ^ (waitTime >>> 32));
    return result;
  }

  @Override
  public String toString() {
    return "RebalanceRuleInformation{"
        + "ruleId=" + ruleId
        + ", ruleName='" + ruleName + '\''
        + ", absoluteTimeJson=" + absoluteTimeJson
        + ", waitTime=" + waitTime
        + ", poolIds=" + poolIds
        + '}';
  }
}
