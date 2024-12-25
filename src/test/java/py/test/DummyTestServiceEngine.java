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

package py.test;

import java.lang.reflect.Constructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.app.context.AppContextImpl;
import py.app.thrift.ThriftAppEngine;
import py.common.struct.EndPoint;
import py.instance.PortType;

public class DummyTestServiceEngine {
  private static Logger logger = LoggerFactory.getLogger(DummyTestServiceEngine.class);

  public static ThriftAppEngine startEngine(DummyTestServiceConfig cfg) throws Exception {
    return startEngine(DummyTestServiceImpl.class, cfg);
  }

  public static ThriftAppEngine startEngine(Class<?> serviceImplClazz, DummyTestServiceConfig cfg)
      throws Exception {
    logger.info("start the dummy testing server");
    Constructor<?> clazzCtor = serviceImplClazz.getConstructor(DummyTestServiceConfig.class);
    DummyTestServiceImpl service = (DummyTestServiceImpl) (clazzCtor.newInstance(cfg));
    ThriftAppEngine engine = new ThriftAppEngine(service, cfg.isBlocking());
    engine.setMaxNetworkFrameSize(cfg.getReceiveMaxFromeSize());
    engine.setMaxNumThreads(cfg.getMaxNumThreads());
    engine.setMinNumThreads(cfg.getMaxNumThreads());
    AppContextImpl context = new AppContextImpl("testing server");
    context.putEndPoint(PortType.CONTROL, new EndPoint(null, cfg.getServicePort()));
    engine.setContext(context);
    engine.setNumWorkerThreads(cfg.getNumWorkerThreads());
    engine.start();
    return engine;
  }

  public static void stop(ThriftAppEngine engine) throws InterruptedException {
    if (engine != null) {
      logger.debug("stopping the engine {}", engine);
      engine.stop();
      Thread.sleep(2000L);
    }
  }
}
