

package py.archive.brick;

public enum BrickStatus {
  allocated(0),
  shadowPageAllocated(1),
  free(2);

  private int value;

  BrickStatus(int value) {
    this.value = value;
  }

  public static BrickStatus findByValue(int value) {
    switch (value) {
      case 0:
        return allocated;
      case 1:
        return shadowPageAllocated;
      case 2:
        return free;
      default:
        return null;
    }
  }

  public int getValue() {
    return value;
  }
}
