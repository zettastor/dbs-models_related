

package py.exception;

import py.datanode.client.DataNodeServiceAsyncClientWrapper.BroadcastResult;

public class FailedToSendBroadcastRequestsException extends Exception {
  private static final long serialVersionUID = 1L;
  private BroadcastResult result;

  public FailedToSendBroadcastRequestsException(BroadcastResult result) {
    super();
    this.result = result;
  }

  public FailedToSendBroadcastRequestsException(String s, BroadcastResult result) {
    super(s);
    this.result = result;
  }

  public BroadcastResult getBroadcastResult() {
    return result;
  }
}
