
package py.utils;

import py.netty.datanode.PyReadResponse;

public interface DataNodeForTestIface {
  void write(long id, byte[] data, long offset, int length);

  boolean containsLog(long id);

  boolean isLogCommitted(long id);

  void commit(long id);

  void setPcl(long pcl);

  PyReadResponse read(long[] offsets, int[] lengths);

}
