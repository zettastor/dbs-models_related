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

package py.icshare;

import java.util.List;
import org.hibernate.SessionFactory;
import org.hibernate.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.annotation.Transactional;

@Transactional
public class InstanceMaintenanceStoreImpl implements InstanceMaintenanceDbStore {
  private static final Logger logger = LoggerFactory.getLogger(InstanceMaintenanceStoreImpl.class);
  private SessionFactory sessionFactory;

  @Override
  public void save(InstanceMaintenanceInformation instanceMaintenanceInformation) {
    sessionFactory.getCurrentSession().saveOrUpdate(instanceMaintenanceInformation);
  }

  @Override
  public void delete(InstanceMaintenanceInformation instanceMaintenanceInformation) {
    sessionFactory.getCurrentSession().delete(instanceMaintenanceInformation);
  }

  @Override
  public InstanceMaintenanceInformation getById(long instanceId) {
    return sessionFactory.getCurrentSession().get(InstanceMaintenanceInformation.class, instanceId);
  }

  @Override
  public void deleteById(long instanceId) {
    Query query = sessionFactory.getCurrentSession().createQuery(
        "delete InstanceMaintenanceInformation where instanceId = :instanceId");
    query.setParameter("instanceId", instanceId);
    query.executeUpdate();
  }

  @Override
  public void clear() {
    sessionFactory.getCurrentSession().createQuery("delete from InstanceMaintenanceInformation")
        .executeUpdate();
  }

  @Override
  public List<InstanceMaintenanceInformation> listAll() {
    return sessionFactory.getCurrentSession().createQuery("from InstanceMaintenanceInformation")
        .list();
  }

  public SessionFactory getSessionFactory() {
    return sessionFactory;
  }

  public void setSessionFactory(SessionFactory sessionFactory) {
    this.sessionFactory = sessionFactory;
  }
}