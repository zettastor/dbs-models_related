
package py.icshare.exception;

public class ConfigurationFormatException extends Exception {
  private static final long serialVersionUID = 1L;

  public ConfigurationFormatException() {
    super();
  }

  public ConfigurationFormatException(String message) {
    super(message);
  }

  public ConfigurationFormatException(String message, Throwable cause) {
    super(message, cause);
  }

  public ConfigurationFormatException(Throwable cause) {
    super(cause);
  }
}
