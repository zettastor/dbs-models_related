

package py.icshare.qos;

import java.util.List;
import py.thrift.share.PoolAlreadyAppliedRebalanceRuleExceptionThrift;
import py.thrift.share.RebalanceRuleNotExistExceptionThrift;

public interface RebalanceRuleStore {
  public void update(RebalanceRuleInformation rebalanceRule);

  public void save(RebalanceRuleInformation rebalanceRule);

  public RebalanceRuleInformation get(long ruleId);

  public List<RebalanceRuleInformation> getAppliedRules();

  public RebalanceRuleInformation getRuleOfPool(long poolId);

  public List<RebalanceRuleInformation> list();

  public void applyRule(RebalanceRuleInformation rebalanceRule, List<Long> poolIdList)
      throws PoolAlreadyAppliedRebalanceRuleExceptionThrift, RebalanceRuleNotExistExceptionThrift;

  public void unApplyRule(RebalanceRuleInformation rebalanceRule, List<Long> poolIdList)
      throws RebalanceRuleNotExistExceptionThrift;

  public int delete(long ruleId);
}
