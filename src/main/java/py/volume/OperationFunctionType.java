
package py.volume;

public enum OperationFunctionType {
  extendVolume(1),
  deleteVolume(2),
  recycleVolume(3),
  fixVolume(4),
  launchDriver(10),
  umountDriver(11),

  //
  recycleVolumeInfo(14);

  private final int value;

  OperationFunctionType(int value) {
    this.value = value;
  }
}
