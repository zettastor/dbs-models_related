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
