

package py.exception;

public class InappropriateSegmentUnitTaskContextException extends RuntimeException {
  private static final long serialVersionUID = 1L;

  public InappropriateSegmentUnitTaskContextException() {
    super();
  }

  public InappropriateSegmentUnitTaskContextException(String message) {
    super(message);
  }

  public InappropriateSegmentUnitTaskContextException(String message, Throwable cause) {
    super(message, cause);
  }

  public InappropriateSegmentUnitTaskContextException(Throwable cause) {
    super(cause);
  }
}
