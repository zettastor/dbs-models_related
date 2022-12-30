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