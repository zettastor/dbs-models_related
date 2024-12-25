

package py.archive.segment.recurring;

import py.archive.segment.SegId;

public abstract class ContextKey extends Object {
  protected final SegId segId;

  public ContextKey(SegId segId) {
    this.segId = segId;
  }

  public SegId getSegId() {
    return segId;
  }

  @Override
  public abstract boolean equals(Object obj);

  @Override
  public abstract int hashCode();

  @Override
  public String toString() {
    return "ContextKey [segId=" + segId + "]";
  }
}
