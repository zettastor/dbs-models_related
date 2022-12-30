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

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.hibernate.Query;
import org.hibernate.SessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.annotation.Transactional;
import py.RequestResponseHelper;
import py.thrift.share.PoolAlreadyAppliedRebalanceRuleExceptionThrift;
import py.thrift.share.RebalanceRuleNotExistExceptionThrift;

@Transactional
public class RebalanceRuleStoreImpl implements RebalanceRuleStore {
  private static final Logger logger = LoggerFactory.getLogger(RebalanceRuleStoreImpl.class);

  private SessionFactory sessionFactory;

  public void setSessionFactory(SessionFactory sessionFactory) {
    this.sessionFactory = sessionFactory;
  }

  @Override
  public void update(RebalanceRuleInformation rebalanceRule) {
    if (rebalanceRule == null) {
      return;
    }

    RebalanceRuleInformation dbRule = get(rebalanceRule.getRuleId());
    if (dbRule != null) {
      dbRule.copy(rebalanceRule);
    }
  }

  @Override
  public void save(RebalanceRuleInformation rebalanceRule) {
    if (rebalanceRule == null) {
      return;
    }

    RebalanceRuleInformation dbRule = get(rebalanceRule.getRuleId());
    if (dbRule == null) {
      sessionFactory.getCurrentSession().saveOrUpdate(rebalanceRule);
    } else {
      dbRule.copy(rebalanceRule);
    }
  }

  @Override
  public RebalanceRuleInformation get(long ruleId) {
    return sessionFactory.getCurrentSession().get(RebalanceRuleInformation.class, ruleId);
  }

  @Override
  public List<RebalanceRuleInformation> getAppliedRules() {
    return sessionFactory.getCurrentSession()
        .createQuery("from py.icshare.qos.RebalanceRuleInformation where poolIds like ',_%,' ")
        .list();
  }

  @Override
  public RebalanceRuleInformation getRuleOfPool(long poolId) {
    Query query = sessionFactory.getCurrentSession()
        .createQuery("from py.icshare.qos.RebalanceRuleInformation where poolIds like :id");
    query.setString("id", "%," + String.valueOf(poolId) + ",%");
    List<RebalanceRuleInformation> resultList = query.getResultList();
    if (resultList == null || resultList.isEmpty()) {
      return null;
    } else {
      return resultList.get(0);
    }
  }

  @Override
  public List<RebalanceRuleInformation> list() {
    return sessionFactory.getCurrentSession()
        .createQuery("from py.icshare.qos.RebalanceRuleInformation").list();
  }

  @Override
  public void applyRule(RebalanceRuleInformation rebalanceRule, List<Long> applyPoolIdList) throws
      PoolAlreadyAppliedRebalanceRuleExceptionThrift, RebalanceRuleNotExistExceptionThrift {
    for (long poolId : applyPoolIdList) {
      //if pool is already applied with a rule
      RebalanceRuleInformation existRule = getRuleOfPool(poolId);
      if (existRule != null && rebalanceRule.getRuleId() != existRule.getRuleId()) {
        RebalanceRule rule = existRule.toRebalanceRule();
        logger.error("apply rebalance rule failed! pool:{} is already applied with rule:{}", poolId,
            rule);
        PoolAlreadyAppliedRebalanceRuleExceptionThrift exceptionThrift =
            new PoolAlreadyAppliedRebalanceRuleExceptionThrift();
        exceptionThrift
            .setRebalanceRule(RequestResponseHelper.convertRebalanceRule2RebalanceRuleThrift(rule));
        throw exceptionThrift;
      }
    }

    RebalanceRuleInformation rule = get(rebalanceRule.getRuleId());
    if (rule == null) {
      logger
          .error("apply rebalance rule failed! rule:{} is not exists!", rebalanceRule.getRuleId());
      throw new RebalanceRuleNotExistExceptionThrift();
    }

    Set<Long> poolIdInStoreSet = new HashSet<>(
        RebalanceRuleInformation.poolIdStr2PoolIdList(rule.getPoolIds()));
    poolIdInStoreSet.addAll(applyPoolIdList);

    String poolIdStr = RebalanceRuleInformation.pooIdList2PoolIdStr(poolIdInStoreSet);
    rule.setPoolIds(poolIdStr);
  }

  @Override
  public void unApplyRule(RebalanceRuleInformation rebalanceRule, List<Long> unApplyPoolIdList)
      throws RebalanceRuleNotExistExceptionThrift {
    RebalanceRuleInformation rule = get(rebalanceRule.getRuleId());
    if (rule == null) {
      logger
          .error("apply rebalance rule failed! rule:{} is not exists!", rebalanceRule.getRuleId());
      throw new RebalanceRuleNotExistExceptionThrift();
    }

    Set<Long> poolIdInStoreSet = new HashSet<>(
        RebalanceRuleInformation.poolIdStr2PoolIdList(rule.getPoolIds()));
    poolIdInStoreSet.removeAll(unApplyPoolIdList);

    String poolIdStr = RebalanceRuleInformation.pooIdList2PoolIdStr(poolIdInStoreSet);
    rule.setPoolIds(poolIdStr);
  }

  @Override
  public int delete(long ruleId) {
    Query query = sessionFactory.getCurrentSession()
        .createQuery("delete py.icshare.qos.RebalanceRuleInformation where ruleId = :id");
    query.setLong("id", ruleId);
    return query.executeUpdate();
  }
}
