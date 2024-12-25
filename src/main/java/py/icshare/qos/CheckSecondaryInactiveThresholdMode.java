
package py.icshare.qos;

public enum CheckSecondaryInactiveThresholdMode {
  AbsoluteTime(1), RelativeTime(2);

  private int value;

  CheckSecondaryInactiveThresholdMode(int value) {
    this.value = value;
  }
}
