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

import java.util.List;
import org.hibernate.SessionFactory;
import org.hibernate.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.annotation.Transactional;

@Transactional
public class ResourceDbStoreImpl implements ResourceStore {
  private static final Logger logger = LoggerFactory.getLogger(ResourceDbStoreImpl.class);
  private SessionFactory sessionFactory;

  @Override
  public void saveResource(PyResource resource) {
    sessionFactory.getCurrentSession().saveOrUpdate(resource);
  }

  @Override
  public void deleteResource(PyResource resource) {
    sessionFactory.getCurrentSession().delete(resource);
  }

  @Override
  public void deleteResourceById(long resourceId) {
    String hql = "delete from PyResource where resourceId = :id";
    Query query = sessionFactory.getCurrentSession().createQuery(hql);
    query.setParameter("id", resourceId).executeUpdate();
  }

  @Override
  public PyResource getResourceByName(String name) {
    Query query = sessionFactory.getCurrentSession()
        .createQuery("from PyResource where resourceName = :name");
    query.setParameter("name", name);
    return (PyResource) query.uniqueResult();
  }

  @Override
  public PyResource getResourceById(long resourceId) {
    return sessionFactory.getCurrentSession().get(PyResource.class, resourceId);
  }

  @Override
  public List<PyResource> listResources() {
    return sessionFactory.getCurrentSession().createQuery("from PyResource").list();
  }

  @Override
  public void cleanResources() {
    sessionFactory.getCurrentSession().createQuery("delete from PyResource").executeUpdate();
  }

  public void setSessionFactory(SessionFactory sessionFactory) {
    this.sessionFactory = sessionFactory;
  }
}