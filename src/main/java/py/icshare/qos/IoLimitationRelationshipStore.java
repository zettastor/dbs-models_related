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
import py.thrift.share.DriverKeyThrift;

public interface IoLimitationRelationshipStore {
  public void update(IoLimitationRelationshipInformation relationshipInformation);

  public void save(IoLimitationRelationshipInformation relationshipInformation);

  public List<IoLimitationRelationshipInformation> getByDriverKey(DriverKeyThrift driverKey);

  public List<IoLimitationRelationshipInformation> getByRuleId(long ruleId);

  public List<IoLimitationRelationshipInformation> list();

  public int deleteByDriverKey(DriverKeyThrift driverKey);

  public int deleteByRuleId(long ruleId);

  public int deleteByRuleIdandDriverKey(DriverKeyThrift driverKey, long ruleId);

}
