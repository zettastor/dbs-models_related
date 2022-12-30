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

import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.debug.DynamicParamConfig;
import py.thrift.share.GetConfigurationsRequest;
import py.thrift.share.GetConfigurationsResponse;
import py.thrift.share.SetConfigurationsRequest;
import py.thrift.share.SetConfigurationsResponse;

/**
 * this abstract class complete setConfigurations and getConfigurations method. when you want to add
 * setting and getting parameters dynamically function, you only should extend the class.
 *
 * <p>if you need to add your parameter for your service, you should add corresponding param in
 *
 * @date 2018/01/30
 * @see DynamicParamConfig
 */
public abstract class AbstractConfigurationServer implements
    py.thrift.share.DebugConfigurator.Iface {
  private static final Logger logger = LoggerFactory.getLogger(AbstractConfigurationServer.class);

  public SetConfigurationsResponse setConfigurations(SetConfigurationsRequest request) {
    logger.warn("setConfigurations request: {}", request);
    SetConfigurationsResponse response = new SetConfigurationsResponse();
    Map<String, String> configuration = request.getConfigurations();

    for (Map.Entry<String, String> entry : configuration.entrySet()) {
      DynamicParamConfig.getInstance().setParameter(entry.getKey(), entry.getValue());
    }

    response.setRequestId(request.getRequestId());
    logger.warn("setConfigurations response: {}", response);
    return response;
  }

  public GetConfigurationsResponse getConfigurations(GetConfigurationsRequest request) {
    logger.warn("getConfigurations request: {}", request);
    Map<String, String> results = DynamicParamConfig.getInstance().getParameter();

    GetConfigurationsResponse response = new GetConfigurationsResponse();
    response.setRequestId(request.getRequestId());
    response.setResults(results);
    logger.warn("getConfigurations response: {}", response);
    return response;
  }

}
