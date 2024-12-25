

package py.exception;

import java.util.List;

/**
 * when  wtsManager check in but not find the page.
 */
public class PageNotInManagerException extends Exception {
  private static final long serialVersionUID = 1L;

  private List<Integer> errorPages = null;

  public PageNotInManagerException() {
    super();
  }

  public PageNotInManagerException(String s) {
    super(s);
  }

  public List<Integer> getErrorPages() {
    return errorPages;
  }

  public void setErrorPages(List<Integer> errorPages) {
    this.errorPages = errorPages;
  }

}
