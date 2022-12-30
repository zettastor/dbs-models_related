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

import java.util.List;
import org.hibernate.Query;
import org.hibernate.SessionFactory;
import org.springframework.transaction.annotation.Transactional;
import py.thrift.share.DriverKeyThrift;

@Transactional
public class IoLimitationRelationshipStoreImpl implements IoLimitationRelationshipStore {
  private SessionFactory sessionFactory;

  public void setSessionFactory(SessionFactory sessionFactory) {
    this.sessionFactory = sessionFactory;
  }

  @Override
  public void update(IoLimitationRelationshipInformation relationshipInformation) {
    sessionFactory.getCurrentSession().update(relationshipInformation);
  }

  @Override
  public void save(IoLimitationRelationshipInformation relationshipInformation) {
    sessionFactory.getCurrentSession().saveOrUpdate(relationshipInformation);
  }

  @SuppressWarnings("unchecked")
  @Override
  public List<IoLimitationRelationshipInformation> getByDriverKey(DriverKeyThrift driverKey) {
    Query query = sessionFactory.getCurrentSession().createQuery(
        "from IoLimitationRelationshipInformation where driverContainerId = :did and "
            + "volumeId = :vid and snapshotId = :sid and driverType = :type");
    query.setLong("did", driverKey.getDriverContainerId());
    query.setLong("vid", driverKey.getVolumeId());
    query.setInteger("sid", driverKey.getSnapshotId());
    query.setString("type", driverKey.getDriverType().name());

    return query.list();
  }

  @SuppressWarnings("unchecked")
  @Override
  public List<IoLimitationRelationshipInformation> getByRuleId(long ruleId) {
    Query query = sessionFactory.getCurrentSession().createQuery(
        "from IoLimitationRelationshipInformation where ruleId = :id");
    query.setLong("id", ruleId);
    return query.list();
  }

  @SuppressWarnings("unchecked")
  @Override
  public List<IoLimitationRelationshipInformation> list() {
    return sessionFactory.getCurrentSession()
        .createQuery("from IoLimitationRelationshipInformation").list();
  }

  @Override
  public int deleteByDriverKey(DriverKeyThrift driverKey) {
    Query query = sessionFactory.getCurrentSession().createQuery(
        "delete IoLimitationRelationshipInformation where driverContainerId = :did"
            + " and volumeId = :vid and snapshotId = :sid and driverType = :type");
    query.setLong("did", driverKey.getDriverContainerId());
    query.setLong("vid", driverKey.getVolumeId());
    query.setInteger("sid", driverKey.getSnapshotId());
    query.setString("type", driverKey.getDriverType().name());
    return query.executeUpdate();
  }

  @Override
  public int deleteByRuleId(long ruleId) {
    Query query = sessionFactory.getCurrentSession().createQuery(
        "delete IoLimitationRelationshipInformation where ruleId = :id");
    query.setLong("id", ruleId);
    return query.executeUpdate();
  }

  @Override
  public int deleteByRuleIdandDriverKey(DriverKeyThrift driverKey, long ruleId) {
    Query query = sessionFactory.getCurrentSession().createQuery(
        "delete IoLimitationRelationshipInformation where ruleId = :id "
            + "and driverContainerId = :did and volumeId = :vid "
            + "and snapshotId = :sid and driverType = :type");
    query.setLong("id", ruleId);
    query.setLong("did", driverKey.getDriverContainerId());
    query.setLong("vid", driverKey.getVolumeId());
    query.setInteger("sid", driverKey.getSnapshotId());
    query.setString("type", driverKey.getDriverType().name());
    return query.executeUpdate();
  }

}
