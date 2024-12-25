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
import org.hibernate.Query;
import org.hibernate.SessionFactory;
import org.springframework.transaction.annotation.Transactional;

@Transactional
public class MigrationSpeedRelationshipStoreImpl implements MigrationSpeedRelationshipStore {
  private SessionFactory sessionFactory;

  public void setSessionFactory(SessionFactory sessionFactory) {
    this.sessionFactory = sessionFactory;
  }

  @Override
  public void update(MigrationRuleRelationshipInformation relationshipInformation) {
    sessionFactory.getCurrentSession().update(relationshipInformation);
  }

  @Override
  public void save(MigrationRuleRelationshipInformation relationshipInformation) {
    sessionFactory.getCurrentSession().saveOrUpdate(relationshipInformation);
  }

  @SuppressWarnings("unchecked")
  @Override
  public List<MigrationRuleRelationshipInformation> getByStoragePoolId(long storagePoolId) {
    Query query = sessionFactory.getCurrentSession().createQuery(
        "from MigrationRuleRelationshipInformation where storagePoolId = :id");
    query.setLong("id", storagePoolId);
    return query.list();
  }

  @SuppressWarnings("unchecked")
  @Override
  public List<MigrationRuleRelationshipInformation> getByRuleId(long ruleId) {
    Query query = sessionFactory.getCurrentSession().createQuery(
        "from MigrationRuleRelationshipInformation where ruleId = :id");
    query.setLong("id", ruleId);
    return query.list();
  }

  @SuppressWarnings("unchecked")
  @Override
  public List<MigrationRuleRelationshipInformation> list() {
    return sessionFactory.getCurrentSession()
        .createQuery("from MigrationRuleRelationshipInformation").list();
  }

  @Override
  public int deleteByStoragePoolId(long storagePoolId) {
    Query query = sessionFactory.getCurrentSession().createQuery(
        "delete MigrationRuleRelationshipInformation where storagePoolId = :id");
    query.setLong("id", storagePoolId);
    return query.executeUpdate();
  }

  @Override
  public int deleteByRuleId(long ruleId) {
    Query query = sessionFactory.getCurrentSession().createQuery(
        "delete MigrationRuleRelationshipInformation where ruleId = :id");
    query.setLong("id", ruleId);
    return query.executeUpdate();
  }

  @Override
  public int deleteByRuleIdandStoragePoolId(long storagePoolId, long ruleId) {
    Query query = sessionFactory.getCurrentSession().createQuery(
        "delete MigrationRuleRelationshipInformation where ruleId = :id and storagePoolId = :vid");
    query.setLong("id", ruleId);
    query.setLong("vid", storagePoolId);
    return query.executeUpdate();
  }
}

