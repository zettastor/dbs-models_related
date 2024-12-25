

package py.icshare.exception;

public class SnapshotExistException extends Exception {
  private static final long serialVersionUID = -8423209118838207624L;

  public SnapshotExistException() {
    super();
  }

  public SnapshotExistException(String message) {
    super(message);
  }

  public SnapshotExistException(String message, Throwable cause) {
    super(message, cause);
  }

  public SnapshotExistException(Throwable cause) {
    super(cause);
  }
}
