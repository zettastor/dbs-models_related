

package py.monitorserver.exception;

public class IllegalAlertRuleException extends Exception {
  private static final long serialVersionUID = 1L;

  public IllegalAlertRuleException() {
    super();
  }

  public IllegalAlertRuleException(String message) {
    super(message);
  }

  public IllegalAlertRuleException(String message, Throwable cause) {
    super(message, cause);
  }

  public IllegalAlertRuleException(Throwable cause) {
    super(cause);
  }
}
