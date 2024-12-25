
package py.icshare;

public interface CapacityRecordStore {
  public void saveCapacityRecord(CapacityRecord capacityRecord);

  public CapacityRecord getCapacityRecord() throws Exception;
}
