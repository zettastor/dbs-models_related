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

package py.drivercontainer.client;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.client.thrift.GenericThriftClientFactory;
import py.common.PyService;
import py.common.struct.EndPoint;
import py.exception.GenericThriftClientFactoryException;
import py.instance.Instance;
import py.instance.InstanceStatus;
import py.instance.InstanceStore;
import py.thrift.drivercontainer.service.DriverContainer;

/**
 * factory generate a DriverContainerServiceBlockingClientWrapper.
 *
 */
public class DriverContainerClientFactory {
  private static final Logger logger = LoggerFactory.getLogger(DriverContainerClientFactory.class);

  private static final long DEFAULT_REQUEST_TIMEOUT = 20000; // ms

  private InstanceStore instanceStore;

  private GenericThriftClientFactory<DriverContainer.Iface> genericClientFactory;

  public DriverContainerClientFactory() {
    genericClientFactory = GenericThriftClientFactory.create(DriverContainer.Iface.class);
  }

  public DriverContainerClientFactory(int workThreadCount) {
    genericClientFactory = GenericThriftClientFactory
        .create(DriverContainer.Iface.class, workThreadCount);
  }

  public InstanceStore getInstanceStore() {
    return instanceStore;
  }

  public void setInstanceStore(InstanceStore instanceStore) {
    this.instanceStore = instanceStore;
  }

  public DriverContainerServiceBlockingClientWrapper build(EndPoint eps)
      throws GenericThriftClientFactoryException {
    return build(eps, DEFAULT_REQUEST_TIMEOUT);
  }

  public DriverContainerServiceBlockingClientWrapper build(EndPoint eps, long requestTimeout)
      throws GenericThriftClientFactoryException {
    if (eps == null) {
      return null;
    }

    DriverContainer.Iface client = genericClientFactory.generateSyncClient(eps, requestTimeout);
    return new DriverContainerServiceBlockingClientWrapper(client);
  }

  /**
   * Pick end points of ok instance driver container refer to expect host list.
   *
   * @param hostList the expect host which driver container running on
   * @return end points for ok driver container whose host is expect
   */
  public List<EndPoint> pickEndPointsReferTo(List<String> hostList) {
    List<EndPoint> driverContainerEps = new ArrayList<EndPoint>();

    Set<Instance> driverContainerInstances = instanceStore
        .getAll(PyService.DRIVERCONTAINER.getServiceName(),
            InstanceStatus.HEALTHY);

    for (Instance instance : driverContainerInstances) {
      EndPoint endPoint = instance.getEndPoint();
      if (hostList.contains(endPoint.getHostName())) {
        driverContainerEps.add(endPoint);
      }
    }

    return driverContainerEps;
  }
}
