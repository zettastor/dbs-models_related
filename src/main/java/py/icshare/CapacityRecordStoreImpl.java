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
