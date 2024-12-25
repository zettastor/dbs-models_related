
package py.archive.segment.recurring;

public enum SegmentUnitTaskType {
  Heartbeat,
  PCL,
  PPL,
  ExpirationChecker,
  CopyPage,
  Default;
  public static final SegmentUnitTaskType DEFAULT_TASK_TYPE = Default;
}
