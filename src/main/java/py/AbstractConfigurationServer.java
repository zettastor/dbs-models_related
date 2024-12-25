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
