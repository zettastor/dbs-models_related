/*
 * Copyright (c) 2022-2022. PengYunNetWork
 *
 * This program is free software: you can use, redistribute, and/or modify it
 * under the terms of the GNU Affero General Public License, version 3 or later ("AGPL"),
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 *  without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 *
 *  You should have received a copy of the GNU Affero General Public License along with
 *  this program. If not, see <http://www.gnu.org/licenses/>.
 */

package py.fsservice.client;

import py.client.ClientWrapperFactory;
import py.client.thrift.GenericThriftClientFactory;
import py.common.PyService;
import py.common.struct.EndPoint;
import py.exception.EndPointNotFoundException;
import py.exception.TooManyEndPointFoundException;
import py.thrift.fsserver.service.FileSystemService;

/**
 * client factory for file system service.
 *
 */
public class FsServiceClientFactory extends
    ClientWrapperFactory<FileSystemService.Iface, FsServiceClientWrapper> {
  private EndPoint fsserverEndpoint;

  public FsServiceClientFactory() {
    init();
    genericClientFactory = GenericThriftClientFactory.create(FileSystemService.Iface.class);
  }

  public FsServiceClientFactory(int minWorkThreadCount) {
    init();
    genericClientFactory = GenericThriftClientFactory
        .create(FileSystemService.Iface.class, minWorkThreadCount);
  }

  public FsServiceClientFactory(int minWorkThreadCount, int connectionTimeoutMs) {
    init();
    genericClientFactory = GenericThriftClientFactory
        .create(FileSystemService.Iface.class, minWorkThreadCount)
        .withDefaultConnectionTimeout(connectionTimeoutMs);
  }

  public FsServiceClientFactory(int minWorkThreadCount, int maxWorkThreadCount,
      int connectionTimeoutMs) {
    init();
    genericClientFactory = GenericThriftClientFactory
        .create(FileSystemService.Iface.class, minWorkThreadCount, maxWorkThreadCount)
        .withDefaultConnectionTimeout(connectionTimeoutMs);
  }

  protected EndPoint getEndpoint() throws EndPointNotFoundException, TooManyEndPointFoundException {
    return getFsEndPoint();
  }

  @Override
  protected void init() {
    this.setDelegatable(true);
    this.setClientClass(FileSystemService.Iface.class);
    this.setClientWrapperClass(FsServiceClientWrapper.class);
  }

  public EndPoint getFsEndPoint() {
    return fsserverEndpoint;
  }

  public void setFsEndPoint(EndPoint fsserverEndpoint) {
    this.fsserverEndpoint = fsserverEndpoint;
  }

}
