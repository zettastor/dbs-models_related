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
import java.util.List;
import java.util.Set;
import org.hibernate.SessionFactory;
import org.springframework.transaction.annotation.Transactional;
import py.icshare.AccountMetadata;

@Transactional
public class AccountStoreDbImpl implements AccountStore {
  private SessionFactory sessionFactory;

  @Override
  public AccountMetadata createAccount(String accountName, String password, String accountType,
      long accountId, Set<Role> rolesToAccount) {
    AccountMetadata account = new AccountMetadata(accountName, password, accountType, accountId);
    account.setRoles(rolesToAccount);
    createAccount(account);
    return account;
  }

  public void createAccount(AccountMetadata account) {
    sessionFactory.getCurrentSession().save(account);
  }

  @Override
  public AccountMetadata deleteAccount(Long accountId) {
    AccountMetadata account = this.getAccountById(accountId);
    if (account != null) {
      sessionFactory.getCurrentSession().delete(account);
      return account;
    } else {
      return null;
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public Collection<AccountMetadata> listAccounts() {
    return sessionFactory.getCurrentSession().createQuery("from AccountMetadata").list();
  }

  @Override
  public AccountMetadata authenticateAccount(String accountName, String password) {
    AccountMetadata account = this.getAccount(accountName);
    if (account != null && account.passwordMatch(password)) {
      return account;
    }
    return null;
  }

  public void setSessionFactory(SessionFactory sessionFactory) {
    this.sessionFactory = sessionFactory;
  }

  @SuppressWarnings("unchecked")
  @Override
  public AccountMetadata getAccount(String accountName) {
    List<AccountMetadata> accounts = sessionFactory.getCurrentSession()
        .createQuery("from AccountMetadata where accountName = :accountName")
        .setParameter("accountName", accountName).list();
    if (accounts != null && accounts.size() > 0) {
      //must case sensitive
      for (AccountMetadata accountMetadata : accounts) {
        if (accountMetadata.getAccountName().equals(accountName)) {
          return accountMetadata;
        }
      }
    }
    return null;
  }

  @Override
  public AccountMetadata getAccountById(Long accountId) {
    return (AccountMetadata) sessionFactory.getCurrentSession()
        .get(AccountMetadata.class, accountId);
  }

  @Override
  public void saveAccount(AccountMetadata account) {
    sessionFactory.getCurrentSession().save(account);
  }

  @Override
  public AccountMetadata updateAccount(AccountMetadata account) {
    sessionFactory.getCurrentSession().update(account);
    return account;
  }

  @Override
  public void clearMemoryData() {
    return;
  }

  @Override
  public void deleteAllAccounts() {
    sessionFactory.getCurrentSession().createQuery("delete from AccountMetadata").executeUpdate();
  }
}
