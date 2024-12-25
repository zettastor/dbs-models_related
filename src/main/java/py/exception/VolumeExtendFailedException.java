

package py.exception;

import py.archive.segment.SegmentUnitStatusConflictCause;

public class VolumeExtendFailedException extends Exception {
  private static final long serialVersionUID = 1L;

  private SegmentUnitStatusConflictCause conflictCause;

  public VolumeExtendFailedException(String message) {
    super(message);
  }

  public VolumeExtendFailedException(String message, Throwable cause) {
    super(message, cause);
  }

  public VolumeExtendFailedException(Throwable cause) {
    super(cause);
  }

  public VolumeExtendFailedException(SegmentUnitStatusConflictCause cause) {
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
