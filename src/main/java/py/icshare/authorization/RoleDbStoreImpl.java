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