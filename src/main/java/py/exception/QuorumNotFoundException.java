
package py.exception;

import py.datanode.client.DataNodeServiceAsyncClientWrapper.BroadcastResult;

/**
 * A generic backend Exception, useful for encapsulating all the different Exceptions that could
 * happen.
 */
public class QuorumNotFoundException extends Exception {
  private static final long serialVersionUID = 1L;
  private BroadcastResult result;

  public QuorumNotFoundException(BroadcastResult result) {
    super();
    this.result = result;
  }

  public QuorumNotFoundException(String s, BroadcastResult result) {
    super(s);
    this.result = result;
  }

  public BroadcastResult getBroadcastResult() {
    return result;
  }
}
