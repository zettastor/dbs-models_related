

package py.exception;

public class NoDriverAvailableException extends Exception {
  private static final long serialVersionUID = 1L;

  public NoDriverAvailableException() {
    super();
  }

  public NoDriverAvailableException(String message) {
    super(message);
  }

  public NoDriverAvailableException(String message, Throwable cause) {
    super(message, cause);
  }

  public NoDriverAvailableException(Throwable cause) {
    super(cause);
  }

}
