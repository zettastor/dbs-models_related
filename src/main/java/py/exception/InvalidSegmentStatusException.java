

package py.exception;

import py.archive.segment.SegmentUnitStatus;

public class InvalidSegmentStatusException extends Exception {
  private static final long serialVersionUID = 1L;

  public InvalidSegmentStatusException() {
    super();
  }

  public InvalidSegmentStatusException(SegmentUnitStatus newStatus,
      SegmentUnitStatus currentStatus) {
    super("current status " + currentStatus + " the new status " + newStatus);
  }

  public InvalidSegmentStatusException(String message) {
    super(message);
  }

  public InvalidSegmentStatusException(String message, Throwable cause) {
    super(message, cause);
  }

  public InvalidSegmentStatusException(Throwable cause) {
    super(cause);
  }
}
