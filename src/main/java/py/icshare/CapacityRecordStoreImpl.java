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
import org.hibernate.SessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.annotation.Transactional;

@Transactional
public class CapacityRecordStoreImpl implements CapacityRecordStore, CapacityRecordDbStore {
  private static final Logger logger = LoggerFactory.getLogger(CapacityRecordStoreImpl.class);
  private SessionFactory sessionFactory;
  private CapacityRecord capacityRecord;

  public void setSessionFactory(SessionFactory sessionFactory) {
    this.sessionFactory = sessionFactory;
  }

  @Override
  public void saveToDb(CapacityRecord capacityRecord) {
    CapacityRecordDbInformation capacityRecordPoolInfo = capacityRecord
        .toCapacityRecordDbInformation(this);
    sessionFactory.getCurrentSession().saveOrUpdate(capacityRecordPoolInfo);
  }

  @Override
  public CapacityRecord loadFromDb() throws Exception {
    CapacityRecordDbInformation capacityRecordPoolInfo = sessionFactory
        .getCurrentSession().get(
            CapacityRecordDbInformation.class, CapacityRecord.CapacityRecordID);
    if (capacityRecordPoolInfo == null) {
      return null;
    }

    return capacityRecordPoolInfo.toCapacityRecord();
  }

  @Override
  public void saveCapacityRecord(CapacityRecord capacityRecord) {
    if (capacityRecord == null) {
      logger.warn("Invalid param, please check them");
      return;
    }
    this.capacityRecord = capacityRecord;
    saveToDb(capacityRecord);
  }

  @Override
  public CapacityRecord getCapacityRecord() throws Exception {
    if (this.capacityRecord == null) {
      this.capacityRecord = loadFromDb();
      if (this.capacityRecord == null) {
        this.capacityRecord = new CapacityRecord();
        saveCapacityRecord(this.capacityRecord);
      }
    }
    return this.capacityRecord;
  }

  @Override
  public Blob createBlob(byte[] bytes) {
    if (bytes == null) {
      return null;
    }
    return sessionFactory.getCurrentSession().getLobHelper().createBlob(bytes);
  }

}
