

package py.exception;

public class DiskDegradeException extends Exception {
  private static final long serialVersionUID = 1L;

  public DiskDegradeException() {
    super();
  }

  public DiskDegradeException(String message) {
    super(message);
  }

  public DiskDegradeException(String message, Throwable cause) {
    super(message, cause);
  }

  public DiskDegradeException(Throwable cause) {
    super(cause);
  }

  public DiskDegradeException(String deviceName, long bfrn, long storageExceptionCount) {
    super("Disk " + deviceName + ":" + bfrn + " has thrown " + storageExceptionCount
        + " storage exceptions and is now considered degrade");
  }

}
