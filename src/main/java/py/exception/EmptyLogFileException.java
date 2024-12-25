
package py.exception;

import java.nio.file.Path;

public class EmptyLogFileException extends Exception {
  private Path badFile;

  public EmptyLogFileException(Path file, String errMsg) {
    super(errMsg);
    this.badFile = file;
  }

  public Path getBadFile() {
    return badFile;
  }
}
