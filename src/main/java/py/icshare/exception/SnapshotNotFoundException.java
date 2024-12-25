
package py.icshare.exception;

public class SnapshotNotFoundException extends Exception {
  private static final long serialVersionUID = -8423209118838207625L;

  public SnapshotNotFoundException() {
    super();
  }

  public SnapshotNotFoundException(String message) {
    super(message);
  }

  public SnapshotNotFoundException(String message, Throwable cause) {
    super(message, cause);
  }

  public SnapshotNotFoundException(Throwable cause) {
    super(cause);
  }
}
