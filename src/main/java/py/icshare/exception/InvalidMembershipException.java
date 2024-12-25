
package py.icshare.exception;

public class InvalidMembershipException extends RuntimeException {
  private static final long serialVersionUID = 1L;

  public InvalidMembershipException() {
    super();
  }

  public InvalidMembershipException(String message) {
    super(message);
  }

  public InvalidMembershipException(String message, Throwable cause) {
    super(message, cause);
  }

  public InvalidMembershipException(Throwable cause) {
    super(cause);
  }
}
