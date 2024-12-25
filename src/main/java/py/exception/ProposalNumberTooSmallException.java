
package py.exception;

public class ProposalNumberTooSmallException extends Exception {
  private static final long serialVersionUID = 1L;

  public ProposalNumberTooSmallException() {
    super();
  }

  public ProposalNumberTooSmallException(String message) {
    super(message);
  }

  public ProposalNumberTooSmallException(String message, Throwable cause) {
    super(message, cause);
  }

  public ProposalNumberTooSmallException(Throwable cause) {
    super(cause);
  }
}
