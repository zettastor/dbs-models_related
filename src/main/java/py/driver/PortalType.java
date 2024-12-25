

package py.driver;

public enum PortalType {
  IPV4(1),

  IPV6(2);

  private final int value;

  private PortalType(int value) {
    this.value = value;
  }

  /**
   * Find corresponding instance of {@link PortalType} to the given value.
   *
   * @return instance of {@link PortalType} if found, otherwise null value will be returned.
   */
  public static PortalType findByValue(int value) {
    for (PortalType portType : PortalType.values()) {
      if (value == portType.value) {
        return portType;
      }
    }

    return null;
  }

  public int getValue() {
    return value;
  }
}
