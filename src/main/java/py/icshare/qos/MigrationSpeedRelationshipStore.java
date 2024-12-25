

package py.icshare.qos;

import java.util.List;

public interface MigrationSpeedRelationshipStore {
  public void update(MigrationRuleRelationshipInformation relationshipInformation);

  public void save(MigrationRuleRelationshipInformation relationshipInformation);

  public List<MigrationRuleRelationshipInformation> getByStoragePoolId(long storagePoolId);

  public List<MigrationRuleRelationshipInformation> getByRuleId(long ruleId);

  public List<MigrationRuleRelationshipInformation> list();

  public int deleteByStoragePoolId(long storagePoolId);

  public int deleteByRuleId(long ruleId);

  public int deleteByRuleIdandStoragePoolId(long storagePoolId, long ruleId);

}
