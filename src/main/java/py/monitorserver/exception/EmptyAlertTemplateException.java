

package py.monitorserver.exception;

public class EmptyAlertTemplateException extends Exception {
  private static final long serialVersionUID = 1L;

  public EmptyAlertTemplateException() {
    super();
  }

  public EmptyAlertTemplateException(String message) {
    super(message);
  }

  public EmptyAlertTemplateException(String message, Throwable cause) {
    super(message, cause);
  }

  public EmptyAlertTemplateException(Throwable cause) {
    super(cause);
  }

}
