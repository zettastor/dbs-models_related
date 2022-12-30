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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.annotation.Transactional;

@Transactional
public class ApiDbStoreImpl implements ApiStore {
  private static final Logger logger = LoggerFactory.getLogger(ApiDbStoreImpl.class);
  private SessionFactory sessionFactory;

  @Override
  public void saveApi(ApiToAuthorize api) {
    sessionFactory.getCurrentSession().saveOrUpdate(api);
  }

  @Override
  public void deleteApi(ApiToAuthorize api) {
    sessionFactory.getCurrentSession().delete(api);
  }

  @Override
  public void cleanApis() {
    sessionFactory.getCurrentSession().createQuery("delete from ApiToAuthorize").executeUpdate();
  }

  @Override
  public List<ApiToAuthorize> listApis() {
    return sessionFactory.getCurrentSession().createQuery("from ApiToAuthorize").list();
  }

  public void setSessionFactory(SessionFactory sessionFactory) {
    this.sessionFactory = sessionFactory;
  }
}