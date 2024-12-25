

package py.archive.brick;

public class BrickMarkedCannotAllocatePage extends Exception {
  private static final long serialVersionUID = 1L;

  public BrickMarkedCannotAllocatePage() {
    super();
  }

  public BrickMarkedCannotAllocatePage(String message) {
    super(message);
  }

  public BrickMarkedCannotAllocatePage(String message, Throwable cause) {
    super(message, cause);
  }

  public BrickMarkedCannotAllocatePage(Throwable cause) {
    super(cause);
  }

}
