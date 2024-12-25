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
