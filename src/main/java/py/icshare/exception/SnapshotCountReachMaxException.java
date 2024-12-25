
package py.icshare.exception;

public class SnapshotCountReachMaxException extends Exception {
  private static final long serialVersionUID = -8423209118838207623L;

  public SnapshotCountReachMaxException() {
    super();
  }

  public SnapshotCountReachMaxException(String message) {
    super(message);
  }

  public SnapshotCountReachMaxException(String message, Throwable cause) {
    super(message, cause);
  }

  public SnapshotCountReachMaxException(Throwable cause) {
    super(cause);
  }
}
