

package py.exception;

/**
 * This exception indicates that a secondary's lease can't be extended. The reason is that the
 * secondary can't be a secondary anymore for some reasons. However, it is bad to change its status
 * to Start immediately without waiting for its primary gets timed out.
 *
 * <p>Therefore, segment unit object can throw this exception when someone wants to extends its
 * lease
 *
 */
public class LeaseExtensionFrozenException extends Exception {
  private static final long serialVersionUID = 1L;

  public LeaseExtensionFrozenException() {
    super();
  }

  public LeaseExtensionFrozenException(String message) {
    super(message);
  }

  public LeaseExtensionFrozenException(String message, Throwable cause) {
    super(message, cause);
  }

  public LeaseExtensionFrozenException(Throwable cause) {
    super(cause);
  }
}
