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
