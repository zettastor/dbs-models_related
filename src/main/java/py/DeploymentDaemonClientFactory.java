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

package py;

import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.client.thrift.GenericThriftClientFactory;
import py.common.struct.EndPoint;
import py.dd.DeploymentDaemonClientWrapper;
import py.exception.GenericThriftClientFactoryException;
import py.thrift.deploymentdaemon.DeploymentDaemon;

/**
 * Factory that generate a deployment daemon client
 *
 * <p>when build a client, you should specify daemon server host and port.
 */
public class DeploymentDaemonClientFactory {
  private static final Logger logger = LoggerFactory.getLogger(DeploymentDaemonClientFactory.class);

  private long thriftTimeout = 180000;

  private GenericThriftClientFactory<DeploymentDaemon.Iface> genericThriftClientFactory;

  public DeploymentDaemonClientFactory() {
    this.genericThriftClientFactory = GenericThriftClientFactory
        .create(DeploymentDaemon.Iface.class);
  }

  public DeploymentDaemonClientFactory(int workThreadCount) {
    this.genericThriftClientFactory = GenericThriftClientFactory
        .create(DeploymentDaemon.Iface.class,
            workThreadCount);
  }

  public DeploymentDaemonClientWrapper build(String host, int port)
      throws GenericThriftClientFactoryException {
    EndPoint endpoint = new EndPoint(host, port);
    return build(endpoint);
  }

  public DeploymentDaemonClientWrapper build(String host, int port, long thriftTimeout)
      throws GenericThriftClientFactoryException {
    EndPoint endpoint = new EndPoint(host, port);
    return build(endpoint, thriftTimeout);
  }

  public DeploymentDaemonClientWrapper build(EndPoint endpoint)
      throws GenericThriftClientFactoryException {
    return build(endpoint, thriftTimeout);
  }

  public DeploymentDaemonClientWrapper build(EndPoint endpoint, long thriftTimeout)
      throws GenericThriftClientFactoryException {
    DeploymentDaemon.Iface client = genericThriftClientFactory
        .generateSyncClient(endpoint, thriftTimeout);

    Validate.notNull(client);

    DeploymentDaemonClientWrapper ddClientWrapper = new DeploymentDaemonClientWrapper(client);
    ddClientWrapper.setPackageTransferSize(genericThriftClientFactory.getNetworkMaxFrameSize());

    return ddClientWrapper;
  }

  public void close() {
    if (genericThriftClientFactory != null) {
      genericThriftClientFactory.close();
    }
  }

  public long getThriftTimeout() {
    return thriftTimeout;
  }

  public void setThriftTimeout(long thriftTimeout) {
    this.thriftTimeout = thriftTimeout;
  }
}
