
package py.exception;

/**
 * A generic backend Exception, useful for encapsulating all the different Exceptions that could
 * happen.
 */

public class PageLockedException extends Exception {
  private static final long serialVersionUID = 1L;

  public PageLockedException() {
    super();
  }

  public PageLockedException(String s) {
    super(s);
  }

  public PageLockedException(Throwable ex1) {
    super(ex1);
  }

  public PageLockedException(String s, Throwable ex1) {
    super(s, ex1);
  }
}
