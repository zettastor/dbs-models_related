
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
