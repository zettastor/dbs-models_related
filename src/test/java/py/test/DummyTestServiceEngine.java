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
