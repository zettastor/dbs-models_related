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
