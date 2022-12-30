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
