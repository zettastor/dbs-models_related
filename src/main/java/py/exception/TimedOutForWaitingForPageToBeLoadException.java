

package py.exception;

public class TimedOutForWaitingForPageToBeLoadException extends Exception {
  private static final long serialVersionUID = 1L;

  public TimedOutForWaitingForPageToBeLoadException() {
    super();
  }

  public TimedOutForWaitingForPageToBeLoadException(String message) {
    super(message);
  }

  public TimedOutForWaitingForPageToBeLoadException(String message, Throwable cause) {
    super(message, cause);
  }

  public TimedOutForWaitingForPageToBeLoadException(Throwable cause) {
    super(cause);
  }
}
