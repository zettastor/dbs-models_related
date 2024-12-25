
package py.exception;

public class SnapshotVersionMismatchException extends Exception {
  private static final long serialVersionUID = 1L;
  private byte[] mySnapshotManagerInBinary;

  public SnapshotVersionMismatchException() {
    super();
  }

  public SnapshotVersionMismatchException(byte[] mySnapshotManagerInBinary) {
    this.mySnapshotManagerInBinary = mySnapshotManagerInBinary;
  }

  public byte[] getMySnapshotManagerInBinary() {
    return mySnapshotManagerInBinary;
  }

  public void setMySnapshotManagerInBinary(byte[] mySnapshotManagerInBinary) {
    this.mySnapshotManagerInBinary = mySnapshotManagerInBinary;
  }
}
