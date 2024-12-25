
package py.exception;

import py.membership.SegmentMembership;

public class StaleMembershipException extends Exception {
  private static final long serialVersionUID = 1L;

  public StaleMembershipException() {
    super();
  }

  public StaleMembershipException(SegmentMembership newMembership,
      SegmentMembership currentMembership) {
    super("the new membership " + newMembership + " has smaller version than the current one "
        + currentMembership);
  }

  public StaleMembershipException(String message) {
    super(message);
  }

  public StaleMembershipException(String message, Throwable cause) {
    super(message, cause);
  }

  public StaleMembershipException(Throwable cause) {
    super(cause);
  }
}
