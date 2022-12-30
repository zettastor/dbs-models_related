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

package py.scriptcontainer.client;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Set;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.common.Constants;
import py.common.struct.EndPoint;
import py.exception.EndPointNotFoundException;
import py.exception.TooManyEndPointFoundException;
import py.instance.Instance;
import py.instance.InstanceStatus;
import py.instance.InstanceStore;
import py.instance.PortType;
import py.thrift.scriptcontainer.service.ScriptContainer;

/**
 * A class to build script container client.
 *
 */
public class ScriptContainerClientFactory {
  private static final long DEFAULT_REQUEST_TIMEOUT = 20000L;
  private static Logger logger = LoggerFactory.getLogger(ScriptContainerClientFactory.class);
  private String scriptContainerInstanceName = Constants.SCRIPT_CONTAINER_INSTANCE_NAME;

  /*
   * the place the latest info of instances in system stored
   */
  private InstanceStore instanceStore;

  /*
   * backup the last accessable endpoint in memory
   */
  private EndPoint endPoint;

  public ScriptContainerClientWrapper build() throws Exception {
    EndPoint endPoint = getEndpoint();
    return build(endPoint);
  }

  public ScriptContainerClientWrapper build(EndPoint endpoint) throws Exception {
    return build(endpoint, DEFAULT_REQUEST_TIMEOUT);
  }

  /**
   * build a scriptcontainer client wrapper connecting to service running on endpoint, build a
   * client manually here but not generic cliet factory due to cannot find framed transport and
   * compact protocol support for perl server.
   */
  public ScriptContainerClientWrapper build(EndPoint endpoint, long requestTimeout)
      throws Exception {
    TSocket transport = new TSocket(endpoint.getHostName(), endpoint.getPort());
    transport.setTimeout((int) requestTimeout);
    transport.open();
    TProtocol protocol = new TBinaryProtocol(transport);
    ScriptContainer.Iface client = new ScriptContainer.Client(protocol);
    ScriptContainerClientWrapper clientWrapper = new ScriptContainerClientWrapper(client);
    return clientWrapper;
  }

  private EndPoint getEndpoint()
      throws EndPointNotFoundException, TooManyEndPointFoundException, Exception {
    // To avoid getting the local script container from dih, use the existing one stored in local
    // memory.
    if (endPoint != null) {
      try {
        ScriptContainer.Iface client = build(endPoint).getClient();
        client.ping();

        return endPoint;
      } catch (Exception e) {
        logger.warn("Endpoint {} for script container has changed, get a new one", endPoint);
      }
    }

    Set<Instance> instances = instanceStore.getAll(
        scriptContainerInstanceName, InstanceStatus.HEALTHY);

    if (instances == null || instances.size() == 0) {
      throw new EndPointNotFoundException(
          "can't find any ok instances named " + scriptContainerInstanceName);
    }

    Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();

    List<String> localHostNames = new ArrayList<String>();
    for (; networkInterfaces.hasMoreElements(); ) {
      NetworkInterface e = networkInterfaces.nextElement();

      Enumeration<InetAddress> a = e.getInetAddresses();
      for (; a.hasMoreElements(); ) {
        InetAddress addr = a.nextElement();
        localHostNames.add(addr.getHostAddress());
      }
    }

    for (Instance instance : instances) {
      endPoint = instance.getEndPointByServiceName(PortType.CONTROL);
      if (endPoint != null && localHostNames.contains(endPoint.getHostName())) {
        return endPoint;
      }
    }

    throw new EndPointNotFoundException();
  }

  public String getScriptContainerInstanceName() {
    return scriptContainerInstanceName;
  }

  public void setScriptContainerInstanceName(String scriptContainerInstanceName) {
    this.scriptContainerInstanceName = scriptContainerInstanceName;
  }

  public InstanceStore getInstanceStore() {
    return instanceStore;
  }

  public void setInstanceStore(InstanceStore instanceStore) {
    this.instanceStore = instanceStore;
  }

  public class ScriptContainerClientWrapper {
    private final ScriptContainer.Iface delegate;

    public ScriptContainerClientWrapper(ScriptContainer.Iface client) {
      this.delegate = client;
    }

    public ScriptContainer.Iface getClient() {
      return delegate;
    }
  }
}
