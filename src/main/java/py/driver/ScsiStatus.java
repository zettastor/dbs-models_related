
package py.driver;

public enum ScsiStatus {
  CREATING(1), NORMAL(2), RECOVERY(3), UMOUNT(4), ERROR(5);

  private int value;

  private ScsiStatus(int value) {
    this.value = value;
  }

  public int getValue() {
    return value;
  }

  public void setValue(int value) {
    this.value = value;
  }

}
