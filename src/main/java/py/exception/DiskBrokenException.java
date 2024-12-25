
package py.exception;

public class DiskBrokenException extends Exception {
  private static final long serialVersionUID = 1L;

  public DiskBrokenException() {
    super();
  }

  public DiskBrokenException(String message) {
    super(message);
  }

  public DiskBrokenException(String message, Throwable cause) {
    super(message, cause);
  }

  public DiskBrokenException(Throwable cause) {
    super(cause);
  }

  public DiskBrokenException(String deviceName, long bfrn, long storageExceptionCount) {
    super("Disk " + deviceName + ":" + bfrn + " has thrown " + storageExceptionCount
        + " storage exceptions and is now considered bad");
  }
}
