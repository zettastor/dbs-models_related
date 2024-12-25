
package py.exception;

public class TimedOutForWaitingForFreePageException extends Exception {
  private static final long serialVersionUID = 1L;

  public TimedOutForWaitingForFreePageException() {
    super();
  }

  public TimedOutForWaitingForFreePageException(String message) {
    super(message);
  }

  public TimedOutForWaitingForFreePageException(String message, Throwable cause) {
    super(message, cause);
  }

  public TimedOutForWaitingForFreePageException(Throwable cause) {
    super(cause);
  }
}
