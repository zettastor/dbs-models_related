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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.common.RequestIdBuilder;
import py.thrift.scriptcontainer.service.ExecuteCommandRequest;
import py.thrift.scriptcontainer.service.ScriptContainer;

/**
 * A class as script container client, in which a request was built and send to server.
 *
 */
public class ScriptContainerClientHandler {
  private static final Logger logger = LoggerFactory.getLogger(ScriptContainerClientHandler.class);

  private ScriptContainerClientFactory scriptContainerClientFactory = new
      ScriptContainerClientFactory();

  public boolean executeCommand(String cmdAsParam) {
    logger.debug("Going to execute command {}", cmdAsParam);

    ExecuteCommandRequest request = new ExecuteCommandRequest();
    request.setRequestId(RequestIdBuilder.get());
    request.setCommand(cmdAsParam);

    ScriptContainer.Iface client = null;
    try {
      client = scriptContainerClientFactory.build().getClient();
    } catch (Exception e) {
      logger.error("Caught an exception when build client of script container service", e);
      return false;
    }

    try {
      // send request to server
      client.executeCommand(request);
    } catch (Exception e) {
      logger.error("Caught an exception when exec command {}", cmdAsParam, e);
      return false;
    }

    return true;
  }

  public ScriptContainerClientFactory getScriptContainerClientFactory() {
    return scriptContainerClientFactory;
  }

  public void setScriptContainerClientFactory(
      ScriptContainerClientFactory scriptContainerClientFactory) {
    this.scriptContainerClientFactory = scriptContainerClientFactory;
  }
}
