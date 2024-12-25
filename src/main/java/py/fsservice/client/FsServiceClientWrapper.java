

package py.fsservice.client;

import py.thrift.fsserver.service.FileSystemService;

public class FsServiceClientWrapper {
  private final FileSystemService.Iface delegate;

  public FsServiceClientWrapper(FileSystemService.Iface client) {
    this.delegate = client;
  }

  public FileSystemService.Iface getDelegate() {
    return delegate;
  }

}
