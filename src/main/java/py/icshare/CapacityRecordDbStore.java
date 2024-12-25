
package py.icshare;

import java.sql.Blob;

public interface CapacityRecordDbStore {
  public void saveToDb(CapacityRecord capacityRecord);

  public CapacityRecord loadFromDb() throws Exception;

  public Blob createBlob(byte[] bytes);

}
