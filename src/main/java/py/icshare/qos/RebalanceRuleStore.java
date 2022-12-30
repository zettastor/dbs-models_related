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
