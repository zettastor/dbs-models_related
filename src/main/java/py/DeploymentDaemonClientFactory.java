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
