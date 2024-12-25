
package py.exception;

/**
 * when archive plug in ,the archive type not  Support.
 */
public class ArchiveTypeNotSupportException extends Exception {
  private static final long serialVersionUID = 1L;

  public ArchiveTypeNotSupportException() {
    super();
  }

  public ArchiveTypeNotSupportException(String message) {
    super(message);
  }
}
