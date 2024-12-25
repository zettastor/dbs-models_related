
package py.driver;

public enum DriverState {
  READY(1), ACTIVE(2);

  private int value;

  private DriverState(int value) {
    this.value = value;
  }

  public int getValue() {
    return value;
  }

  public void setValue(int value) {
    this.value = value;
  }

}
