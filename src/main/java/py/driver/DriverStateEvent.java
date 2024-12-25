

package py.driver;

/**
 * An enum to list all driver state event to change driver status.
 */
public enum DriverStateEvent {
  ACCEPT_MOUNT_REQUEST(1), ACCEPT_UMOUNT_REQUEST(2), DRIVER_SERVER_PROCESS_UP(3), DRIVER_PROCESS_UP(
      4), DRIVER_PROCESS_DOWN(5), TIMEOUT(6), REPORTTIMEOUT(
      7), NEWREPORT(8);

  private int value;

  private DriverStateEvent(int value) {
    this.value = value;
  }

  public int getValue() {
    return value;
  }

  public void setValue(int value) {
    this.value = value;
  }
}
