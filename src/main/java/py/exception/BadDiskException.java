

package py.exception;

public class BadDiskException extends Exception {
  private static final long serialVersionUID = 1L;

  public BadDiskException() {
    super();
  }

  public BadDiskException(String message) {
    super(message);
  }

  public BadDiskException(String message, Throwable cause) {
    super(message, cause);
  }

  public BadDiskException(Throwable cause) {
    super(cause);
  }

  public BadDiskException(String deviceName, long bfrn, long storageExceptionCount) {
    super("Disk " + deviceName + ":" + bfrn + " has thrown " + storageExceptionCount
        + " storage exceptions and is now considered bad");
  }
}
