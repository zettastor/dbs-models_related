/**
 * Copyright (C) 2013-2024 Nanjing Pengyun Network Technology Co., Ltd.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
