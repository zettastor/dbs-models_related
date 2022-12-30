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

