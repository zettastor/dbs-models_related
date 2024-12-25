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
