
package py.archive.segment.recurring;

public class DefaultSegmentUnitTaskCallback implements SegmentUnitTaskCallback {
  public void removing(SegmentUnitTaskContext targetContext) {
    if (targetContext != null) {
      targetContext.setAbandonedTask(true);
    }
  }
}
