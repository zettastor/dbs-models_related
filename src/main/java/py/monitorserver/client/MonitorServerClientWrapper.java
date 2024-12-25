
package py.monitorserver.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.thrift.monitorserver.service.MonitorServer;

/**
 * a wrapper class of {@code MonitorServer.Iface}.
 */
public class MonitorServerClientWrapper {
  private static final Logger logger = LoggerFactory.getLogger(MonitorServerClientWrapper.class);
  private final MonitorServer.Iface delegate;

  public MonitorServerClientWrapper(MonitorServer.Iface client) {
    this.delegate = client;
  }

  public MonitorServer.Iface getClient() {
    return delegate;
  }
}
