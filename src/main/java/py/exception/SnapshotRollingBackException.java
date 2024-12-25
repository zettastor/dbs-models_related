

package py.exception;

public class SnapshotRollingBackException extends Exception {
  private static final long serialVersionUID = 1L;
  private String mySnpMgrJson;

  public SnapshotRollingBackException() {
    super();
  }

  public SnapshotRollingBackException(String mySnpMgrJson) {
    this.setMySnpMgrJson(mySnpMgrJson);
  }

  public String getMySnpMgrJson() {
    return mySnpMgrJson;
  }

  public void setMySnpMgrJson(String mySnpMgrJson) {
    this.mySnpMgrJson = mySnpMgrJson;
  }
}
