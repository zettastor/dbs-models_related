

package py.exception;

import py.archive.segment.SegmentUnitStatusConflictCause;

public class SegmentUnitStatusConflictExeption extends Exception {
  private static final long serialVersionUID = 1L;

  private SegmentUnitStatusConflictCause conflictCause;

  public SegmentUnitStatusConflictExeption(String message) {
    super(message);
  }

  public SegmentUnitStatusConflictExeption(String message, Throwable cause) {
    super(message, cause);
  }

  public SegmentUnitStatusConflictExeption(Throwable cause) {
    super(cause);
  }

  public SegmentUnitStatusConflictExeption(SegmentUnitStatusConflictCause cause) {
    super(cause.toString());
    this.conflictCause = cause;
  }

  public SegmentUnitStatusConflictCause getConflictCause() {
    return conflictCause;
  }

  public void setConflictCause(SegmentUnitStatusConflictCause conflictCause) {
    this.conflictCause = conflictCause;
  }
}
