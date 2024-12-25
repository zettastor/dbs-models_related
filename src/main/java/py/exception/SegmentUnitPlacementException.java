
package py.exception;

import py.archive.segment.SegId;

public class SegmentUnitPlacementException extends Exception {
  private static final long serialVersionUID = 1L;

  public SegmentUnitPlacementException(String archive, SegId segId, long size) {
    super(
        "No space on archive " + archive + " for volume " + segId.toString() + " of size " + size);
  }

  public SegmentUnitPlacementException(SegId segId, long size) {
    super(
        "Could not find space on any archive for volume " + segId.toString() + " of size " + size);
  }
}
