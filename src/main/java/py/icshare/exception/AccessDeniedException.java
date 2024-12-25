

package py.icshare.exception;

public class AccessDeniedException extends Exception {
  private static final long serialVersionUID = -1065415306395856777L;

  public AccessDeniedException() {
    super();
  }

  public AccessDeniedException(String message, Throwable cause, boolean enableSuppression,
      boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }

  public AccessDeniedException(String message, Throwable cause) {
    super(message, cause);
  }

  public AccessDeniedException(String message) {
    super(message);
  }

  public AccessDeniedException(Throwable cause) {
    super(cause);
  }

}
