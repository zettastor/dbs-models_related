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

import java.io.IOException;
import java.sql.Blob;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.hibernate.SessionFactory;
import org.hibernate.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.annotation.Transactional;
import py.informationcenter.Status;
import py.informationcenter.StoragePool;
import py.informationcenter.StoragePoolDbStore;
import py.informationcenter.StoragePoolInformationDb;
import py.informationcenter.StoragePoolStore;

@Transactional
public class StoragePoolStoreImpl implements StoragePoolStore, StoragePoolDbStore {
  private static final Logger logger = LoggerFactory.getLogger(StoragePoolStoreImpl.class);
  private SessionFactory sessionFactory;
  private Map<Long, StoragePool> storagePoolMap = new ConcurrentHashMap<Long, StoragePool>();

  public void setSessionFactory(SessionFactory sessionFactory) {
    this.sessionFactory = sessionFactory;
  }

  @Override
  public void saveStoragePoolToDb(StoragePool storagePool) {
    if (storagePool == null) {
      logger.warn("Invalid param, please check them");
      return;
    }

    StoragePoolInformationDb storagePoolInfo = storagePool.toStoragePoolInformationDb(this);
    sessionFactory.getCurrentSession().saveOrUpdate(storagePoolInfo);
  }

  @Override
  public void deleteStoragePoolFromDb(Long storagePoolId) {
    Query query = sessionFactory.getCurrentSession().createQuery(
        "delete StoragePoolInformationDb where poolId = :poolId");
    query.setLong("poolId", storagePoolId);
    query.executeUpdate();
  }

  @Override
  public StoragePool getStoragePoolFromDb(Long storagePoolId) throws SQLException, IOException {
    StoragePoolInformationDb storagePoolInfo = (StoragePoolInformationDb) sessionFactory
        .getCurrentSession().get(
            StoragePoolInformationDb.class, storagePoolId);
    if (storagePoolInfo == null) {
      return null;
    }

    return storagePoolInfo.toStoragePool();
  }

  @Override
  public void reloadAllStoragePoolsFromDb() throws SQLException, IOException {
    @SuppressWarnings("unchecked")
    List<StoragePoolInformationDb> allStoragePoolsInDb = sessionFactory.getCurrentSession()
        .createQuery("from StoragePoolInformationDb").list();
    for (StoragePoolInformationDb storagePoolInfo : allStoragePoolsInDb) {
      storagePoolMap.put(storagePoolInfo.getPoolId(), storagePoolInfo.toStoragePool());
    }
  }

  @Override
  public boolean saveStoragePool(StoragePool storagePool) {
    if (storagePool == null) {
      logger.warn("Invalid param, please check them");
      return false;
    }

    StoragePool currentPool = storagePoolMap.get(storagePool.getPoolId());
    if (currentPool != null) {
      if (currentPool.getStatus().equals(Status.Deleting) && !storagePool.getStatus()
          .equals(Status.Deleting)) {
        storagePool.setStatus(Status.Deleting);
        logger.warn(
            "storage pool in db status is Deleting, cannot change it to other status. "
                + "pool in db:{} "
                + "; will changed pool:{}",
            currentPool, storagePool, new Exception());
      }
    }

    logger.info("saveStoragePool, the  save pool:{}", storagePool);
    // save to memory
    storagePoolMap.put(storagePool.getPoolId(), storagePool);
    // save to Db
    saveStoragePoolToDb(storagePool);
    return true;
  }

  @Override
  public void deleteStoragePool(Long storagePoolId) {
    // delete from memory
    storagePoolMap.remove(storagePoolId);
    // delete from Db
    deleteStoragePoolFromDb(storagePoolId);
  }

  @Override
  public StoragePool getStoragePool(Long storagePoolId) throws SQLException, IOException {
    StoragePool storagePool = storagePoolMap.get(storagePoolId);
    if (storagePool == null) {
      storagePool = getStoragePoolFromDb(storagePoolId);
    }
    return storagePool;
  }

  @Override
  public List<StoragePool> listAllStoragePools() throws SQLException, IOException {
    List<StoragePool> allStorages = new ArrayList<>();
    // if infocenter restart, should reload storagePools from Db
    if (storagePoolMap.isEmpty()) {
      reloadAllStoragePoolsFromDb();
    }
    allStorages.addAll(storagePoolMap.values());
    return allStorages;
  }

  @Override
  public List<StoragePool> listStoragePools(List<Long> storagePoolIds)
      throws SQLException, IOException {
    List<StoragePool> storagePools = new ArrayList<StoragePool>();
    // if infocenter restart, should reload storagePools from Db
    if (storagePoolMap.isEmpty()) {
      reloadAllStoragePoolsFromDb();
    }
    for (Long storagePoolId : storagePoolIds) {
      if (storagePoolMap.containsKey(storagePoolId)) {
        storagePools.add(storagePoolMap.get(storagePoolId));
      }
    }
    return storagePools;
  }

  @Override
  public List<StoragePool> listStoragePools(Long domainId) throws SQLException, IOException {
    List<StoragePool> storagePools = new ArrayList<StoragePool>();
    // if infocenter restart, should reload storagePools from Db
    if (storagePoolMap.isEmpty()) {
      reloadAllStoragePoolsFromDb();
    }
    for (StoragePool storagePool : storagePoolMap.values()) {
      if (storagePool.getDomainId().equals(domainId)) {
        storagePools.add(storagePool);
      }
    }
    return storagePools;
  }

  @Override
  public void clearMemoryMap() {
    storagePoolMap.clear();
  }

  @Override
  public void removeArchiveFromStoragePool(Long storagePoolId, Long datanodeId, Long archiveId)
      throws SQLException, IOException {
    StoragePool storagePool = getStoragePool(storagePoolId);
    if (storagePool == null) {
      logger.warn("do not have storagePool:{}", storagePoolId);
      return;
    }
    storagePool.removeArchiveFromDatanode(datanodeId, archiveId);
    saveStoragePool(storagePool);
  }

  @Override
  public void deleteVolumeId(Long storagePoolId, Long volumeId) throws SQLException, IOException {
    StoragePool storagePool = getStoragePool(storagePoolId);
    storagePool.removeVolumeId(volumeId);
  }

  @Override
  public void addVolumeId(Long storagePoolId, Long volumeId) throws SQLException, IOException {
    StoragePool storagePool = getStoragePool(storagePoolId);
    storagePool.addVolumeId(volumeId);
    saveStoragePoolToDb(storagePool);
  }

  @Override
  public String toString() {
    return "StoragePoolStoreImpl [sessionFactory=" + sessionFactory + ", storagePoolMap="
        + storagePoolMap + "]";
  }

  @Override
  public Blob createBlob(byte[] bytes) {
    if (bytes == null) {
      return null;
    }
    return sessionFactory.getCurrentSession().getLobHelper().createBlob(bytes);
  }
}
