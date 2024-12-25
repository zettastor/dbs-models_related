
package py.exception;

public class UnableToUpdateMembershipException extends Exception {
  private static final long serialVersionUID = 1L;

  public UnableToUpdateMembershipException() {
    super();
  }

  public UnableToUpdateMembershipException(String message) {
    super(message);
  }

  public UnableToUpdateMembershipException(String message, Throwable cause) {
    super(message, cause);
  }

  public UnableToUpdateMembershipException(Throwable cause) {
    super(cause);
  }
}
