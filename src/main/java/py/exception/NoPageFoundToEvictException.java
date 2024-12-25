

package py.exception;

/**
 * A generic backend Exception, useful for encapsulating all the different Exceptions that could
 * happen.
 */
public class NoPageFoundToEvictException extends Exception {
  private static final long serialVersionUID = 1L;

  public NoPageFoundToEvictException(String s) {
    super(s);
  }

  public NoPageFoundToEvictException(Throwable ex1) {
    super(ex1);
  }

  public NoPageFoundToEvictException(String s, Throwable ex1) {
    super(s, ex1);
  }
}
