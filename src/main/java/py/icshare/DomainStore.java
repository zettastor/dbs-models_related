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

package py.icshare;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

public interface DomainStore {
  /**
   * save domain to memory map.
   */
  public boolean saveDomain(Domain domain);

  /**
   * delete domain from memory map.
   */
  public void deleteDomain(Long domainId);

  public void removeDatanodeFromDomain(Long domainId, Long datanodeInstanceId)
      throws SQLException, IOException;

  /**
   * get domain from memory map.
   */
  public Domain getDomain(Long domainId) throws SQLException, IOException;

  /**
   * list all domains from memory map.
   */
  public List<Domain> listAllDomains() throws SQLException, IOException;

  /**
   * list some domains from memory map.
   */
  public List<Domain> listDomains(List<Long> domainIds) throws SQLException, IOException;

  /**
   * clear all memory map to sync data from database again.
   */
  public void clearMemoryMap();
}
