

package py.exception;

/**
 * A generic backend Exception, useful for encapsulating all the different Exceptions that could
 * happen.
 */
public class PageIsDirtyException extends Exception {
  private static final long serialVersionUID = 1L;

  public PageIsDirtyException(String s) {
    super(s);
  }

  public PageIsDirtyException(Throwable ex1) {
    super(ex1);
  }

  public PageIsDirtyException(String s, Throwable ex1) {
    super(s, ex1);
  }
}
