
package py.monitorcenter.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.infocenter.client.InformationCenterClientFactory;
import py.thrift.systemmonitor.service.MonitorCenter;

/**
 * a wrapper class of {@code MonitorCenter.Iface}.
 */
public class MonitorCenterClientWrapper {
  private static final Logger logger = LoggerFactory
      .getLogger(InformationCenterClientFactory.class);
  private final MonitorCenter.Iface delegate;

  public MonitorCenterClientWrapper(MonitorCenter.Iface client) {
    this.delegate = client;
  }

  public MonitorCenter.Iface getClient() {
    return delegate;
  }

}
