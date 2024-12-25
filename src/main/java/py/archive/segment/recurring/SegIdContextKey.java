
package py.archive.segment.recurring;

import py.archive.segment.SegId;

/**
 * Compared with other context keys, this is a special context key that represents a segId.
 *
 */
public class SegIdContextKey extends ContextKey {
  public SegIdContextKey(SegId segId) {
    super(segId);
  }

  @Override
  public boolean equals(Object obj) {
    return segId.equals(obj);
  }

  @Override
  public int hashCode() {
    return segId.hashCode();
  }
}
