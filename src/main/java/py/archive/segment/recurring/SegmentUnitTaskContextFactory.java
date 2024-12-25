
package py.archive.segment.recurring;

import java.util.List;
import py.archive.segment.SegId;

public interface SegmentUnitTaskContextFactory {
  /**
   * generate a list of contexts related to the segment.
   */
  public List<SegmentUnitTaskContext> generateProcessingContext(SegId segId);

  /**
   * generate a list of new contexts according to the processing result of the previous round Note
   * that if a null or an empty list is returned, then no context will be added to the system.
   */
  public List<SegmentUnitTaskContext> generateProcessingContext(SegmentUnitProcessResult result);
}
