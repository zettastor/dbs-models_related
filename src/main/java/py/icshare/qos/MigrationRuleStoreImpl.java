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
public class MigrationRuleStoreImpl implements MigrationRuleStore {
  private SessionFactory sessionFactory;

  public void setSessionFactory(SessionFactory sessionFactory) {
    this.sessionFactory = sessionFactory;
  }

  @Override
  public void update(MigrationRuleInformation migrationSpeedInformation) {
    sessionFactory.getCurrentSession().update(migrationSpeedInformation);
  }

  @Override
  public void save(MigrationRuleInformation migrationSpeedInformation) {
    sessionFactory.getCurrentSession().saveOrUpdate(migrationSpeedInformation);
  }

  @Override
  public MigrationRuleInformation get(long ruleId) {
    return (MigrationRuleInformation) sessionFactory.getCurrentSession()
        .get(MigrationRuleInformation.class, ruleId);
  }

  @SuppressWarnings("unchecked")
  @Override
  public List<MigrationRuleInformation> list() {
    return sessionFactory.getCurrentSession()
        .createQuery("from py.icshare.qos.MigrationRuleInformation").list();
  }

  @Override
  public int delete(long ruleId) {
    Query query = sessionFactory.getCurrentSession()
        .createQuery("delete py.icshare.qos.MigrationRuleInformation where ruleId = :id");
    query.setLong("id", ruleId);
    return query.executeUpdate();
  }
}