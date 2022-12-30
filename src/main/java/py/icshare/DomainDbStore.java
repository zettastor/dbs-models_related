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
import java.sql.Blob;
import java.sql.SQLException;

public interface DomainDbStore {
  /**
   * save domain to database.
   */
  public void saveDomainToDb(Domain domain);

  /**
   * delete domain from database by domainId.
   */
  public void deleteDomainFromDb(Long domainId);

  /**
   * get domain from database by domainId.
   */
  public Domain getDomainFromDb(Long domainId) throws SQLException, IOException;

  /**
   * reload all domains from database.
   */
  public void reloadAllDomainsFromDb() throws SQLException, IOException;

  public Blob createBlob(byte[] bytes);
}