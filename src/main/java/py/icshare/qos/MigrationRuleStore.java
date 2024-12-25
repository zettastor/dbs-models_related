
package py.icshare.qos;

import java.util.List;

public interface MigrationRuleStore {
  public void update(MigrationRuleInformation migrationInformation);

  public void save(MigrationRuleInformation migrationInformation);

  public MigrationRuleInformation get(long ruleId);

  public List<MigrationRuleInformation> list();

  public int delete(long ruleId);
}
