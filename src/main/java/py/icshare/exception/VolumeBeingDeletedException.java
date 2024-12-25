

package py.icshare.exception;

public class VolumeBeingDeletedException extends Exception {
  private static final long serialVersionUID = -6985431265725746690L;

  public VolumeBeingDeletedException() {
    super();
  }

  public VolumeBeingDeletedException(String message, Throwable cause, boolean enableSuppression,
      boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }

  public VolumeBeingDeletedException(String message, Throwable cause) {
    super(message, cause);
  }

  public VolumeBeingDeletedException(String message) {
    super(message);
  }

  public VolumeBeingDeletedException(Throwable cause) {
    super(cause);
  }

}
