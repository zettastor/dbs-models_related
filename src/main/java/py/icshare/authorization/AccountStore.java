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

package py.icshare.authorization;

import java.util.Collection;
import java.util.Set;
import py.icshare.AccountMetadata;

public interface AccountStore {
  public AccountMetadata createAccount(String accountName, String password, String accountType,
      long accountId,
      Set<Role> rolesToAccount);

  public AccountMetadata deleteAccount(Long accountId);

  public AccountMetadata getAccount(String accountName);

  public AccountMetadata getAccountById(Long accountId);

  public AccountMetadata updateAccount(AccountMetadata account);

  public Collection<AccountMetadata> listAccounts();

  public AccountMetadata authenticateAccount(String accountName, String password);

  public void clearMemoryData();

  public void deleteAllAccounts();

  public void saveAccount(AccountMetadata account);
}
