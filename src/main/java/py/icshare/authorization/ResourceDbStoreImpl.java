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
