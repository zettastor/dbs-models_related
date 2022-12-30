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

import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.client.thrift.GenericThriftClientFactory;
import py.common.PyService;
import py.common.struct.EndPoint;
import py.exception.EndPointNotFoundException;
import py.exception.GenericThriftClientFactoryException;
import py.exception.TooManyEndPointFoundException;
import py.instance.Instance;
import py.instance.InstanceId;
import py.instance.InstanceStatus;
import py.instance.InstanceStore;
import py.instance.PortType;
import py.thrift.coordinator.service.Coordinator;

/**
 * coordinator client factory.
 *
 */
public class CoordinatorClientFactory {
  private static final Logger logger = LoggerFactory.getLogger(CoordinatorClientFactory.class);

  private static final long DEFAULT_REQUEST_TIMEOUT = 20000L;

  private String coordinatorName = PyService.COORDINATOR.getServiceName();

  private InstanceStore instanceStore;

  private InstanceId coordinatorInstanceId;

  private GenericThriftClientFactory<Coordinator.Iface> genericClientFactory;

  public CoordinatorClientFactory() {
    genericClientFactory = GenericThriftClientFactory.create(Coordinator.Iface.class);
  }

  public CoordinatorClientFactory(int minWorkThreadCount) {
    genericClientFactory = GenericThriftClientFactory
        .create(Coordinator.Iface.class, minWorkThreadCount);
  }

  public CoordinatorClientFactory(int minWorkThreadCount, int maxWorkThreadCount) {
    genericClientFactory = GenericThriftClientFactory
        .create(Coordinator.Iface.class, minWorkThreadCount,
            maxWorkThreadCount);
  }

  public CoordinatorClientFactory(int minWorkThreadCount, int maxWorkThreadCount,
      int connectionTimeoutMs) {
    genericClientFactory = GenericThriftClientFactory
        .create(Coordinator.Iface.class, minWorkThreadCount,
            maxWorkThreadCount).withDefaultConnectionTimeout(connectionTimeoutMs);
  }

  public void close() {
    if (genericClientFactory != null) {
      logger.debug("we are going to shutdown control center client");
      genericClientFactory.close();
    }
  }

  public CoordinatorClientWrapper build()
      throws EndPointNotFoundException, TooManyEndPointFoundException,
      GenericThriftClientFactoryException {
    EndPoint endPoint = getEndpoint();
    return build(endPoint);
  }

  public CoordinatorClientWrapper build(EndPoint endpoint)
      throws GenericThriftClientFactoryException {
    return build(endpoint, DEFAULT_REQUEST_TIMEOUT);
  }

  /**
   * Build a client wrapper given an coordinator endpoint.
   */
  public CoordinatorClientWrapper build(EndPoint endpoint, long requestTimeout)
      throws GenericThriftClientFactoryException {
    Coordinator.Iface client = genericClientFactory.generateSyncClient(endpoint, requestTimeout);
    CoordinatorClientWrapper clientWrapper = new CoordinatorClientWrapper(client);
    return clientWrapper;
  }

  private EndPoint getEndpoint() throws EndPointNotFoundException, TooManyEndPointFoundException {
    Instance coordinatorInstance = null;

    if (coordinatorInstanceId != null) {
      coordinatorInstance = instanceStore.get(coordinatorInstanceId);
      if (coordinatorInstance.getStatus() == InstanceStatus.HEALTHY) {
        return coordinatorInstance.getEndPointByServiceName(PortType.CONTROL);
      }
    }

    Set<Instance> instances = instanceStore.getAll(coordinatorName, InstanceStatus.HEALTHY);
    if (instances.size() == 0) {
      throw new EndPointNotFoundException("can't find any ok instances named " + coordinatorName);
    } else if (instances.size() > 1) {
      throw new TooManyEndPointFoundException(
          "find " + instances.size() + " control center instance that have "
              + coordinatorName + " and OK status");
    }

    coordinatorInstance = instances.iterator().next();
    coordinatorInstanceId = coordinatorInstance.getId();
    return coordinatorInstance.getEndPointByServiceName(PortType.CONTROL);
  }

  public String getCoordinatorName() {
    return coordinatorName;
  }

  public void setCoordinatorName(String coordinatorName) {
    this.coordinatorName = coordinatorName;
  }

  public InstanceStore getInstanceStore() {
    return instanceStore;
  }

  public void setInstanceStore(InstanceStore instanceStore) {
    this.instanceStore = instanceStore;
  }

  public GenericThriftClientFactory<Coordinator.Iface> getGenericClientFactory() {
    return genericClientFactory;
  }

  public class CoordinatorClientWrapper {
    private final Coordinator.Iface delegate;

    public CoordinatorClientWrapper(Coordinator.Iface client) {
      this.delegate = client;
    }

    public Coordinator.Iface getClient() {
      return delegate;
    }
  }
}
