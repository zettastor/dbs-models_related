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
