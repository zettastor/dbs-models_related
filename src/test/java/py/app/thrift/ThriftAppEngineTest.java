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

package py.app.thrift;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.transport.TTransportException;
import org.jboss.netty.handler.codec.frame.TooLongFrameException;
import org.junit.Test;
import py.app.context.AppContextImpl;
import py.client.thrift.GenericThriftClientFactory;
import py.common.RequestIdBuilder;
import py.common.struct.EndPoint;
import py.instance.PortType;
import py.test.DummyTestServiceAbstract;
import py.test.TestBase;
import py.thrift.testing.service.DummyTestService;
import py.thrift.testing.service.DummyTestService.Iface;
import py.thrift.testing.service.PingRequest;

public class ThriftAppEngineTest extends TestBase {
  public ThriftAppEngineTest() throws Exception {
    super.init();
  }

  public static String bytesTolongsString(byte[] buffer) {
    // convert byte array to string
    ByteBuffer temp = ByteBuffer.wrap(buffer, 0, buffer.length);
    String str = "";
    for (int i = 0; i < buffer.length / 8; i++) {
      str += Long.toString(temp.getLong());
      str += "=";
    }

    return str;
  }

  @Test
  public void testTwoEngineWithOneProcessor() throws Exception {
    final int port = 34000;
    final AtomicInteger receiveCount = new AtomicInteger(0);

    ThriftAppEngine engine = null;
    engine = new ThriftAppEngine(new ThriftProcessorFactory() {
      @Override
      public TProcessor getProcessor() {
        return new DummyTestService.Processor<>(new DummyTestServiceAbstract() {
          @Override
          public String ping(PingRequest request) throws TException {
            receiveCount.incrementAndGet();
            return "i am coming";
          }
        });
      }
    });

    AppContextImpl appContext = new AppContextImpl("test-async") {
      public Map<PortType, EndPoint> getEndPointsThrift() {
        return super.getEndPoints();
      }
    };

    appContext.putEndPoint(PortType.CONTROL, new EndPoint("localhost", port));
    appContext.putEndPoint(PortType.HEARTBEAT, new EndPoint("localhost", port + 1));
    engine.setContext(appContext);

    final GenericThriftClientFactory<Iface> factory = GenericThriftClientFactory
        .create(DummyTestService.Iface.class);

    engine.start();
    int sendCount = 1000;
    Map<PortType, EndPoint> endPoints = appContext.getEndPoints();
    int count = endPoints.size();
    for (int i = 0; i < sendCount; i++) {
      int tmp = i % count;
      // PortType type = null;
      // if (tmp == 0) {
      // type = PortType.CONTROL;
      // } else {
      // type = PortType.HEARTBEAT;
      // }
      DummyTestService.Iface client = null;
      try {
        EndPoint endPoint = endPoints.get(PortType.get(tmp));
        logger.warn("end point:{}", endPoint);
        client = factory.generateSyncClient(endPoint);
      } catch (Exception e) {
        logger.error("caught an exception", e);
      }
      client.ping(new PingRequest(i));
    }

    assertEquals(receiveCount.get(), sendCount);

    engine.stop();
  }

  @Test(expected = TooLongFrameException.class)
  public void testThriftAppEngineMaxFrameSizeDecodeError() throws Throwable {
    final int port = 34000;
    final AtomicInteger receiveCount = new AtomicInteger(0);

    ThriftAppEngine engine;
    engine = new ThriftAppEngine(new ThriftProcessorFactory() {
      @Override
      public TProcessor getProcessor() {
        return new DummyTestService.Processor<>(new DummyTestServiceAbstract() {
          @Override
          public String ping(PingRequest request) throws TException {
            receiveCount.incrementAndGet();
            return "i am coming";
          }
        });
      }
    });

    AppContextImpl appContext = new AppContextImpl("test-async") {
      public Map<PortType, EndPoint> getEndPointsThrift() {
        return super.getEndPoints();
      }
    };

    appContext.putEndPoint(PortType.CONTROL, new EndPoint("localhost", port));
    engine.setContext(appContext);

    final GenericThriftClientFactory<Iface> factory = GenericThriftClientFactory
        .create(DummyTestService.Iface.class);

    factory.setMaxNetworkFrameSize(500);
    engine.start();
    PingRequest pingRequest = new PingRequest();
    pingRequest.setRequestId(RequestIdBuilder.get());

    // test size > MaxNetworkFrameSize + 1024
    byte[] test = new byte[2000];
    Arrays.fill(test, (byte) 8);
    pingRequest.setData(test);

    Map<PortType, EndPoint> endPoints = appContext.getEndPoints();
    EndPoint endPoint = endPoints.get(PortType.get(0));
    DummyTestService.Iface client = factory.generateSyncClient(endPoint);
    try {
      client.ping(pingRequest);
    } catch (TTransportException te) {
      logger.warn("caught an exception: ", te.getCause());
      throw te.getCause();
    } finally {
      engine.stop();
    }
  }

  @Test
  public void testThriftAppEngineMaxFrameSizeEncodeError() throws Exception {
    final int port = 34000;
    final AtomicInteger receiveCount = new AtomicInteger(0);

    ThriftAppEngine engine;
    engine = new ThriftAppEngine(new ThriftProcessorFactory() {
      @Override
      public TProcessor getProcessor() {
        return new DummyTestService.Processor<>(new DummyTestServiceAbstract() {
          @Override
          public String ping(PingRequest request) throws TException {
            byte[] test1 = new byte[2000];
            Arrays.fill(test1, (byte) 3);
            return bytesTolongsString(test1);
          }
        });
      }
    });

    AppContextImpl appContext = new AppContextImpl("test-async") {
      public Map<PortType, EndPoint> getEndPointsThrift() {
        return super.getEndPoints();
      }
    };

    appContext.putEndPoint(PortType.CONTROL, new EndPoint("localhost", port));
    engine.setMaxNetworkFrameSize(500);
    engine.setContext(appContext);

    final GenericThriftClientFactory<Iface> factory = GenericThriftClientFactory
        .create(DummyTestService.Iface.class);

    engine.start();
    PingRequest pingRequest = new PingRequest();
    pingRequest.setRequestId(RequestIdBuilder.get());

    //
    byte[] test = new byte[500];
    Arrays.fill(test, (byte) 8);
    pingRequest.setData(test);

    Map<PortType, EndPoint> endPoints = appContext.getEndPoints();
    EndPoint endPoint = endPoints.get(PortType.get(0));
    DummyTestService.Iface client = null;
    client = factory.generateSyncClient(endPoint);

    boolean catchTestInternalError = false;
    try {
      client.ping(pingRequest);
    } catch (TTransportException te) {
      logger.warn("caught an exception", te.getCause());
      catchTestInternalError = true;
    }

    engine.stop();
    assertTrue(catchTestInternalError);
  }
}
