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
public class RoleDbStoreImpl implements RoleStore {
  private static final Logger logger = LoggerFactory.getLogger(RoleDbStoreImpl.class);
  private SessionFactory sessionFactory;

  @Override
  public void saveRole(Role role) {
    sessionFactory.getCurrentSession().saveOrUpdate(role);
  }

  @Override
  public void deleteRole(Role role) {
    sessionFactory.getCurrentSession().delete(role);
  }

  @Override
  public Role getRoleByName(String name) {
    Query query = sessionFactory.getCurrentSession().createQuery("from Role where name = :name");
    query.setParameter("name", name);
    return (Role) query.uniqueResult();
  }

  @Override
  public Role getRoleById(long roleId) {
    return sessionFactory.getCurrentSession().get(Role.class, roleId);
  }

  @Override
  public List<Role> listRoles() {
    return sessionFactory.getCurrentSession().createQuery("from Role").list();
  }

  @Override
  public void cleanRoles() {
    sessionFactory.getCurrentSession().createQuery("delete from Role").executeUpdate();
  }

  public void setSessionFactory(SessionFactory sessionFactory) {
    this.sessionFactory = sessionFactory;
  }
}
