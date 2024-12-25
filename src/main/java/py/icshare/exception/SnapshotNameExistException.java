

package py.icshare.exception;

public class SnapshotNameExistException extends Exception {
  private static final long serialVersionUID = -8423209118838207624L;

  public SnapshotNameExistException() {
    super();
  }

  public SnapshotNameExistException(String message) {
    super(message);
  }

  public SnapshotNameExistException(String message, Throwable cause) {
    super(message, cause);
  }

  public SnapshotNameExistException(Throwable cause) {
    super(cause);
  }
}
