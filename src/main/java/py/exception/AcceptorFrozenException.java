

package py.exception;

/**
 * The acceptor has been frozen and not accept any requests (prepare and accept messages).
 *
 */
public class AcceptorFrozenException extends Exception {
  private static final long serialVersionUID = 1L;

  public AcceptorFrozenException() {
    super();
  }

  public AcceptorFrozenException(String message) {
    super(message);
  }

  public AcceptorFrozenException(String message, Throwable cause) {
    super(message, cause);
  }

  public AcceptorFrozenException(Throwable cause) {
    super(cause);
  }
}
