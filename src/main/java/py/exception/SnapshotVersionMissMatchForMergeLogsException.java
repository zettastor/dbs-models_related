

package py.exception;

import py.datanode.client.DataNodeServiceAsyncClientWrapper.BroadcastResult;

public class SnapshotVersionMissMatchForMergeLogsException extends Exception {
  private static final long serialVersionUID = 1L;
  private BroadcastResult broadcastResult;

  public SnapshotVersionMissMatchForMergeLogsException(BroadcastResult result) {
    super();
    this.broadcastResult = result;
  }
}
