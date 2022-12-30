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