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

import java.sql.Blob;
import java.util.ArrayList;
import java.util.List;
import org.hibernate.Query;
import org.hibernate.SessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.annotation.Transactional;
import py.io.qos.IoLimitation;
import py.io.qos.IoLimitationInformation;
import py.io.qos.IoLimitationStore;

@Transactional
public class IoLimitationStoreImpl implements IoLimitationStore {
  private static final Logger logger = LoggerFactory.getLogger(IoLimitationStoreImpl.class);
  private SessionFactory sessionFactory;

  public void setSessionFactory(SessionFactory sessionFactory) {
    this.sessionFactory = sessionFactory;
  }

  @Override
  public void update(IoLimitation ioLimitation) {
    IoLimitationInformation ioLimitationInformation = ioLimitation.toIoLimitationInformation(this);
    sessionFactory.getCurrentSession().saveOrUpdate(ioLimitationInformation);
  }

  @Override
  public void save(IoLimitation ioLimitation) {
    IoLimitationInformation ioLimitationInformation = ioLimitation.toIoLimitationInformation(this);
    sessionFactory.getCurrentSession().saveOrUpdate(ioLimitationInformation);
  }

  @Override
  public IoLimitation get(long ioLimitationId) {
    IoLimitationInformation ioLimitationInformation = sessionFactory.getCurrentSession()
        .get(IoLimitationInformation.class, ioLimitationId);

    return ioLimitationInformation.toIoLimitation();
  }

  @SuppressWarnings("unchecked")
  @Override
  public List<IoLimitation> list() {
    List<IoLimitation> result = new ArrayList<>();
    List<IoLimitationInformation> ioLimitationList = sessionFactory.getCurrentSession()
        .createQuery("from IoLimitationInformation").list();

    for (IoLimitationInformation ioLimitationInformation : ioLimitationList) {
      result.add(ioLimitationInformation.toIoLimitation());
    }

    return result;
  }

  @Override
  public int delete(long ruleId) {
    Query query = sessionFactory.getCurrentSession()
        .createQuery("delete IoLimitationInformation where ruleId = :id");
    query.setLong("id", ruleId);
    return query.executeUpdate();
  }

  @Override
  public Blob createBlob(byte[] bytes) {
    if (bytes == null || bytes.length == 0) {
      return null;
    }
    return this.sessionFactory.getCurrentSession().getLobHelper().createBlob(bytes);
  }
}
