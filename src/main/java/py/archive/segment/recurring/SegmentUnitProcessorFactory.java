

package py.archive.segment.recurring;

public interface SegmentUnitProcessorFactory {
  public SegmentUnitProcessor generate(SegmentUnitTaskContext context);
}
