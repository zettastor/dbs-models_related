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
