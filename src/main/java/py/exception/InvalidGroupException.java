

package py.exception;

public class InvalidGroupException extends Exception {
  private static final long serialVersionUID = 1L;

  public InvalidGroupException() {
    super();
  }

  public InvalidGroupException(String message) {
    super(message);
  }

  public InvalidGroupException(String message, Throwable cause) {
    super(message, cause);
  }

  public InvalidGroupException(Throwable cause) {
    super(cause);
  }

}
