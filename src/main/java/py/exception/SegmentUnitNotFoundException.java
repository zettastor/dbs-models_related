
package py.exception;

public class SegmentUnitNotFoundException extends Exception {
  private static final long serialVersionUID = 1L;

  public SegmentUnitNotFoundException(String s) {
    super(s);
  }

  public SegmentUnitNotFoundException(Throwable ex1) {
    super(ex1);
  }

  public SegmentUnitNotFoundException(String s, Throwable ex1) {
    super(s, ex1);
  }
}
