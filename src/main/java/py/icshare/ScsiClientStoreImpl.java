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

import java.sql.Blob;
import java.util.ArrayList;
import java.util.List;
import org.hibernate.SessionFactory;
import org.hibernate.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.annotation.Transactional;
import py.informationcenter.ScsiClient;

@Transactional
public class ScsiClientStoreImpl implements ScsiClientStore {
  private static final Logger logger = LoggerFactory.getLogger(ScsiClientStoreImpl.class);
  private SessionFactory sessionFactory;

  public void setSessionFactory(SessionFactory sessionFactory) {
    this.sessionFactory = sessionFactory;
  }

  @Override
  public void clearDb() {
    List<ScsiClient> scsiClientList = listAllScsiClients();
    List<String> ids = new ArrayList<>();
    for (ScsiClient scsiClient : scsiClientList) {
      ids.add(scsiClient.getScsiClientInfo().getIpName());
    }
    deleteScsiClient(ids);
  }

  @Override
  public void saveOrUpdateScsiClientInfo(ScsiClient scsiClient) {
    if (scsiClient == null) {
      logger.warn("Invalid param, please check them");
      return;
    }
    sessionFactory.getCurrentSession().saveOrUpdate(scsiClient);
  }

  @Override
  public void deleteScsiClient(List<String> ipNames) {
    for (String ip : ipNames) {
      deleteScsiClientByIp(ip);
    }
  }

  @Override
  public int deleteScsiClientInfoByVolumeIdAndSnapshotId(String ipName, long volumeId,
      int snapshotId) {
    Query query = sessionFactory.getCurrentSession().createQuery(
        "delete ScsiClient where scsiClientInfo.volumeId = :id and "
            + "scsiClientInfo.snapshotId = :snapshotId"
            + " and scsiClientInfo.ipName = :ipName");
    query.setParameter("id", volumeId);
    query.setParameter("ipName", ipName);
    query.setParameter("snapshotId", snapshotId);
    return query.executeUpdate();
  }

  @Override
  public ScsiClient getScsiClientInfoByVolumeIdAndSnapshotId(String ipName, long volumeId,
      int snapshotId) {
    ScsiClient scsiClient = null;
    Query query = sessionFactory.getCurrentSession().createQuery(
        "from ScsiClient where scsiClientInfo.volumeId = :id and "
            + "scsiClientInfo.snapshotId = :snapshotId"
            + " and scsiClientInfo.ipName = :ipName");
    query.setParameter("id", volumeId);
    query.setParameter("ipName", ipName);
    query.setParameter("snapshotId", snapshotId);

    List<ScsiClient> scsiClientList = (List<ScsiClient>) query.list();
    if (scsiClientList != null && scsiClientList.size() == 1) {
      scsiClient = scsiClientList.get(0);
    }
    return scsiClient;
  }

  @Override
  public List<ScsiClient> listAllScsiClients() {
    return sessionFactory.getCurrentSession().createQuery("from ScsiClient").list();
  }

  @Override
  public List<ScsiClient> getScsiClientByIp(String ipName) {
    Query query = sessionFactory.getCurrentSession()
        .createQuery("from ScsiClient where scsiClientInfo.ipName = :ipName");
    query.setParameter("ipName", ipName);
    return (List<ScsiClient>) query.list();
  }

  @Override
  public int deleteScsiClientByIp(String ipName) {
    if (ipName == null) {
      logger.error("Invalid parameter, ipName:{}", ipName);
      return 0;
    }
    Query query = sessionFactory.getCurrentSession().createQuery(
        "delete ScsiClient where scsiClientInfo.ipName = :id");
    query.setParameter("id", ipName);
    return query.executeUpdate();
  }

  @Override
  public int updateScsiDriverDescription(String ipName, long volumeId, int snapshotId,
      String statusDescription) {
    Query query = sessionFactory.getCurrentSession().createQuery(
        "update ScsiClient set statusDescription = :statusDescription "
            + "where scsiClientInfo.volumeId = :volumeId and "
            + "scsiClientInfo.snapshotId = :snapshotId and scsiClientInfo.ipName = :ipName");
    query.setParameter("ipName", ipName);
    query.setParameter("volumeId", volumeId);
    query.setParameter("snapshotId", snapshotId);
    query.setParameter("statusDescription", statusDescription);

    return query.executeUpdate();
  }

  @Override
  public int updateScsiPydDriverContainerId(String ipName, long volumeId, int snapshotId,
      long driverContainerId) {
    Query query = sessionFactory.getCurrentSession().createQuery(
        "update ScsiClient set driverContainerId = :driverContainerId "
            + "where scsiClientInfo.volumeId = :volumeId and "
            + "scsiClientInfo.snapshotId = :snapshotId and scsiClientInfo.ipName = :ipName");
    query.setParameter("ipName", ipName);
    query.setParameter("volumeId", volumeId);
    query.setParameter("snapshotId", snapshotId);
    query.setParameter("driverContainerId", driverContainerId);

    return query.executeUpdate();
  }

  @Override
  public int updateScsiDriverStatus(String ipName, long volumeId, int snapshotId,
      String scsiDriverStatus) {
    Query query = sessionFactory.getCurrentSession().createQuery(
        "update ScsiClient set scsiDriverStatus = :scsiDriverStatus "
            + "where scsiClientInfo.volumeId = :volumeId and "
            + "scsiClientInfo.snapshotId = :snapshotId and scsiClientInfo.ipName = :ipName");
    query.setParameter("ipName", ipName);
    query.setParameter("volumeId", volumeId);
    query.setParameter("snapshotId", snapshotId);
    query.setParameter("scsiDriverStatus", scsiDriverStatus);

    return query.executeUpdate();
  }

  @Override
  public int updateScsiDriverStatusAndDescription(String ipName, long volumeId, int snapshotId,
      String scsiDriverStatus,
      String statusDescription, String descriptionType) {
    Query query = sessionFactory.getCurrentSession().createQuery(
        "update ScsiClient set scsiDriverStatus = :scsiDriverStatus, "
            + "statusDescription = :statusDescription, descriptionType = :descriptionType"
            + " where scsiClientInfo.volumeId = :volumeId and "
            + "scsiClientInfo.snapshotId = :snapshotId and scsiClientInfo.ipName = :ipName");
    query.setParameter("ipName", ipName);
    query.setParameter("volumeId", volumeId);
    query.setParameter("snapshotId", snapshotId);
    query.setParameter("scsiDriverStatus", scsiDriverStatus);
    query.setParameter("statusDescription", statusDescription);
    query.setParameter("descriptionType", descriptionType);

    return query.executeUpdate();
  }

  @Override
  public Blob createBlob(byte[] bytes) {
    if (bytes == null) {
      return null;
    }
    return sessionFactory.getCurrentSession().getLobHelper().createBlob(bytes);
  }
}
