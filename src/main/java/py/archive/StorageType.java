
package py.archive;

/**
 * classify the storage type by io speeding or mechanical principle.
 */
public enum StorageType {
  SATA(1), SAS(2), SSD(3), PCIE(4);

  private final int value;

  StorageType(int value) {
    this.value = value;
  }

  public static StorageType findByValue(int value) {
    switch (value) {
      case 1:
        return SATA;
      case 2:
        return SAS;
      case 3:
        return SSD;
      case 4:
        return PCIE;
      default:
        return null;
    }
  }

  public int getValue() {
    return value;
  }
}
