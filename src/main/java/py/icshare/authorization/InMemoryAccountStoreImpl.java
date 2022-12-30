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

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.icshare.AccountMetadata;

/**
 * This account store is used as a cache for the sqlite database account store
 * 
 * <p>The account name is unique across the whole system.
 *
 */
public class InMemoryAccountStoreImpl implements AccountStore {
  private static final Logger logger = LoggerFactory.getLogger(InMemoryAccountStoreImpl.class);
  private AccountStore dbAccountStore;
  private Table<Long, String, AccountMetadata> accountTableInMem = HashBasedTable
      .<Long, String, AccountMetadata>create();
  private boolean allInCache = false;

  @Override
  //TODO: ensure that don't create a duplicated account with the same account id
  //TODO: ensure the account type is correct
  public synchronized AccountMetadata createAccount(String accountName, String password,
      String accountType,
      long accountId, Set<Role> rolesToAccount) {
    if (accountName == null || accountType == null) {
      logger.debug("Can't create an account whose accountName or accountType is null");
      return null;
    }

    AccountMetadata account = getAccount(accountName);
    if (account == null) {
      if (dbAccountStore == null) {
        account = new AccountMetadata(accountName, password, accountType, accountId);
        account.setRoles(rolesToAccount);
      } else {
        account = dbAccountStore
            .createAccount(accountName, password, accountType, accountId, rolesToAccount);
      }

      accountTableInMem.put(accountId, accountName, account);
      return account;
    } else {
      //the account is already existed
      return null;
    }
  }

  @Override
  public synchronized AccountMetadata deleteAccount(Long accountId) {
    AccountMetadata account = this.getAccountById(accountId);
    if (account != null) {
      if (dbAccountStore != null) {
        dbAccountStore.deleteAccount(accountId);
      }
      accountTableInMem.remove(accountId, account.getAccountName());
      return account;
    } else {
      return null;
    }
  }

  @Override
  public synchronized Collection<AccountMetadata> listAccounts() {
    if (allInCache || dbAccountStore == null) {
      return accountTableInMem.values();
    } else {
      Collection<AccountMetadata> allAccounts = dbAccountStore.listAccounts();
      if (allAccounts != null) {
        for (AccountMetadata account : allAccounts) {
          if (accountTableInMem.get(account.getAccountId(), account.getAccountName()) == null) {
            // put non-existing account to the table in memory
            accountTableInMem.put(account.getAccountId(), account.getAccountName(), account);
          }
        }
      } else {
        // create an empty list
        allAccounts = new ArrayList<AccountMetadata>();
      }
      allInCache = true;
      return allAccounts;
    }
  }

  @Override
  public synchronized AccountMetadata authenticateAccount(String accountName, String password) {
    logger.warn("authenticateAccount, password:{}", password);
    AccountMetadata account = this.getAccount(accountName);
    if (account != null && account.passwordMatch(password)) {
      logger.warn("authenticateAccount, the accountMetadata:{},{}", account.getHashedPassword(),
          account.passwordMatch(password));
      return account;
    }
    return null;
  }

  @Override
  public synchronized AccountMetadata getAccount(String accountName) {
    Map<Long, AccountMetadata> id2AccountMap = accountTableInMem.column(accountName);
    logger.warn("the getAccount id2AccountMap:{},{}", id2AccountMap, accountTableInMem);
    if (id2AccountMap == null || id2AccountMap.size() == 0) {
      // Not in the memory, let's check it in the db

      AccountMetadata accountInDb = null;
      if (dbAccountStore != null) {
        accountInDb = dbAccountStore.getAccount(accountName);
        if (accountInDb != null) {
          // not in the memory but in the db. Put it to the memory
          accountTableInMem.put(accountInDb.getAccountId(), accountName, accountInDb);
        }
      }
      return accountInDb;
    } else {
      // in the memory
      if (id2AccountMap.size() > 1) {
        logger.warn(
            "there are mulitple accounts associated with the same account name {} the first "
                + "account is returned",
            accountName);
      }
      logger.warn("getAccount, the id2AccountMap bean:{},{}", id2AccountMap.values().size(),
          id2AccountMap.values().iterator().next().getHashedPassword());
      return id2AccountMap.values().iterator().next();
    }
  }

  @Override
  public synchronized AccountMetadata getAccountById(Long accountId) {
    Map<String, AccountMetadata> name2AccountMap = accountTableInMem.row(accountId);
    logger.info("the getAccountById name2AccountMap:{}", name2AccountMap);
    if (name2AccountMap == null || name2AccountMap.size() == 0) {
      // Not in the memory, let's check it in the db

      AccountMetadata accountInDb = null;
      if (dbAccountStore != null) {
        accountInDb = dbAccountStore.getAccountById(accountId);
        logger.info("the getAccountById accountInDb:{}", accountInDb);
        if (accountInDb != null) {
          // not in the memory but in the db. Put it to the memory
          accountTableInMem.put(accountId, accountInDb.getAccountName(), accountInDb);
        }
      }
      return accountInDb;
    } else {
      // in the memory
      if (name2AccountMap.size() > 1) {
        logger
            .warn("there are mulitple accounts associated with the same account id {}", accountId);
      }

      return name2AccountMap.values().iterator().next();
    }
  }

  @Override
  public void saveAccount(AccountMetadata account) {
    if (null == account) {
      return;
    }
    if (null != dbAccountStore) {
      dbAccountStore.deleteAccount(account.getAccountId());
      dbAccountStore.saveAccount(account);
    }
    accountTableInMem.remove(account.getAccountId(), account.getAccountName());
    accountTableInMem.put(account.getAccountId(), account.getAccountName(), account);

  }

  @Override
  // the old one is going to be replaced instead of merging. This is different from the accountDB
  // implementation
  // we should change accountDb implementation. The account name can't not be changed
  public synchronized AccountMetadata updateAccount(AccountMetadata account) {
    if (account == null) {
      return null;
    }

    if (dbAccountStore != null) {
      AccountMetadata existingAccountInDb = dbAccountStore.getAccountById(account.getAccountId());
      if (existingAccountInDb == null) {
        //TODO throw an AccountNotFoundException
        return null;
      } else if (!existingAccountInDb.getAccountName().equals(account.getAccountName())) {
        //TODO account name can't be changed
        return null;
      } else {
        dbAccountStore.updateAccount(account);
      }
    }

    AccountMetadata existingAccountInMemory = accountTableInMem
        .remove(account.getAccountId(), account.getAccountName());

    if (existingAccountInMemory != null) {
      // merge it with account
      if (account.getAccountType() != null) {
        existingAccountInMemory.setAccountType(account.getAccountType());
      }
      if (account.getHashedPassword() != null) {
        existingAccountInMemory.setHashedPassword(account.getHashedPassword());
      }
      if (account.getSalt() != null) {
        existingAccountInMemory.setSalt(account.getSalt());
      }
      if (null != account.getRoles()) {
        existingAccountInMemory.setRoles(account.getRoles());
      }
      if (null != account.getResources()) {
        existingAccountInMemory.setResources(account.getResources());
      }
      accountTableInMem
          .put(account.getAccountId(), account.getAccountName(), existingAccountInMemory);

      return account;
    } else {
      //TODO: we should throw an AccountNotFoundException
      return null;
    }
  }

  public synchronized void clearMemoryData() {
    accountTableInMem.clear();
  }

  public AccountStore getAccountStore() {
    return dbAccountStore;
  }

  public void setAccountStore(AccountStore accountStore) {
    this.dbAccountStore = accountStore;
  }

  @Override
  public synchronized void deleteAllAccounts() {
    accountTableInMem.clear();
    if (dbAccountStore != null) {
      dbAccountStore.deleteAllAccounts();
    }
  }
}