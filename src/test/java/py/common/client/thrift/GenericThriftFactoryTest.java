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

package py.common.client.thrift;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.net.InetAddress;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.transport.TTransportException;
import org.jboss.netty.handler.codec.frame.TooLongFrameException;
import org.jboss.netty.handler.timeout.ReadTimeoutException;
import org.junit.Test;
import py.app.thrift.ThriftAppEngine;
import py.client.thrift.GenericThriftClientFactory;
import py.common.PortUtil;
import py.common.RequestIdBuilder;
import py.common.struct.EndPoint;
import py.common.struct.EndPointParser;
import py.exception.GenericThriftClientFactoryException;
import py.test.DummyTestServiceConfig;
import py.test.DummyTestServiceEngine;
import py.test.TestBase;
import py.thrift.testing.service.DummyTestService;
import py.thrift.testing.service.DummyTestService.AsyncClient.ping_call;
import py.thrift.testing.service.PingRequest;

public class GenericThriftFactoryTest extends TestBase {
  public GenericThriftFactoryTest() throws Exception {
    super.init();
  }

  @Test
  public void testExecutor() throws Exception {
    final AtomicBoolean isCalled = new AtomicBoolean();
    final CountDownLatch latch = new CountDownLatch(1);

    ExecutorService executorService = Executors.newSingleThreadExecutor();
    executorService.execute(new Runnable() {
      public void run() {
        isCalled.set(true);
        latch.countDown();
      }
    });

    latch.await();
    assertTrue(isCalled.get());
    executorService.shutdown();
  }

  @Test(expected = TException.class)
  public void testServiceException() throws Exception {
    logger.info("start test: testServiceException");
    DummyTestServiceConfig cfg = DummyTestServiceConfig.create();
    cfg.setReturnObject(new TException("I am a dummy service and always throw an exception"));
    ThriftAppEngine engine = null;
    try {
      engine = DummyTestServiceEngine.startEngine(cfg);
      trySyncClient(cfg);
    } catch (Exception e) {
      logger.error("Caught an Exception", e);
      throw e;
    } finally {
      DummyTestServiceEngine.stop(engine);
    }
  }

  /**
   * Due to some reasons, we can't run multiple trySyncClient() back to back. The opened socket
   * can't be closed attemptly after engine.DummyTestServiceEngine.stop(). Currently, comment out
   * other 3 trySyncClient() and leave only one in order to make the test passing.
   */
  @Test
  public void testSyncClient() throws Exception {
    logger.info("start test: testSyncClient");
    ThriftAppEngine engine = null;
    DummyTestServiceConfig cfg = DummyTestServiceConfig.create();
    try {
      cfg.setReturnObject("ok");
      engine = DummyTestServiceEngine.startEngine(cfg);
      trySyncClient(cfg);
    } catch (Exception e) {
      logger.error("Caught an Exception", e);
      throw e;
    } finally {
      DummyTestServiceEngine.stop(engine);
    }

    try {
      cfg.setReturnObject("hello");
      engine = DummyTestServiceEngine.startEngine(cfg);
      trySyncClient(cfg);
    } catch (Exception e) {
      logger.error("Caught an Exception", e);
      throw e;
    } finally {
      DummyTestServiceEngine.stop(engine);
    }
  }

  @Test
  public void testDelegatableClient() throws Exception {
    logger.info("start test: testSyncClient");
    ThriftAppEngine engine = null;
    DummyTestServiceConfig cfg = DummyTestServiceConfig.create();
    try {
      cfg.setReturnObject("ok");
      engine = DummyTestServiceEngine.startEngine(cfg);

      tryDelegatableSyncClient(cfg);
    } catch (TProtocolException e) {
      logger.debug("Caught an exception", e);
    } catch (Throwable t) {
      logger.error("Caught an exception", t);
    } finally {
      DummyTestServiceEngine.stop(engine);
    }

    engine = DummyTestServiceEngine.startEngine(cfg);
    try {
      cfg.setReturnObject(null);
      trySyncClient(cfg);
      fail();
    } catch (Exception e) {
      assertTrue(e instanceof TApplicationException);
      logger.warn("caught an exception", e);
    } finally {
      DummyTestServiceEngine.stop(engine);
    }
  }

  @Test(expected = TApplicationException.class)
  public void testReturnNull() throws Exception {
    logger.info("start test: testReturnNull");
    ThriftAppEngine engine = null;
    try {
      DummyTestServiceConfig cfg = DummyTestServiceConfig.create();
      cfg.setReturnObject(null);
      engine = DummyTestServiceEngine.startEngine(cfg);
      trySyncClient(cfg);
    } finally {
      DummyTestServiceEngine.stop(engine);
    }
  }

  @Test
  public void testSendExceedPackage() throws Throwable {
    logger.info("start test: testSendExceedPackage");
    ThriftAppEngine engine = null;
    try {
      DummyTestServiceConfig cfg = DummyTestServiceConfig.create();
      cfg.setNetworkTimeout(2000);
      cfg.setReceiveMaxFromeSize(5 * 1024);
      engine = DummyTestServiceEngine.startEngine(cfg);
      trySyncClient(cfg, new byte[6 * 1024]);
      fail("not caught exception");
    } catch (Exception e) {
      logger.warn("++caught an exception", e);
    } finally {
      DummyTestServiceEngine.stop(engine);
    }
  }

  @Test
  public void testReponseExceedPackage() {
    logger.info("start test: testReponseExceedPackage");
    ThriftAppEngine engine = null;
    try {
      DummyTestServiceConfig cfg = DummyTestServiceConfig.create();
      cfg.setSendMaxFrameSize(5 * 1024);
      cfg.setReceiveMaxFromeSize(10 * 1024);
      cfg.setReturnObject(String.valueOf(new char[8 * 1024]));
      engine = DummyTestServiceEngine.startEngine(cfg);
      trySyncClient(cfg);
      assertTrue(false);
    } catch (Exception e) {
      logger.warn("caught an exception", e);
      assertTrue(e.getCause() instanceof TooLongFrameException);
    } finally {
      try {
        DummyTestServiceEngine.stop(engine);
      } catch (InterruptedException e) {
        logger.error("caught an exception", e);
        fail();
      }
    }
  }

  @Test
  public void testTimeout() throws Exception {
    logger.info("start test: testTimeout");
    ThriftAppEngine engine = null;
    long startTime = 0;
    DummyTestServiceConfig cfg = DummyTestServiceConfig.create();
    try {
      cfg.setNetworkTimeout(1000);
      cfg.setLatency(2000);
      engine = DummyTestServiceEngine.startEngine(cfg);

      startTime = System.currentTimeMillis();
      trySyncClient(cfg);
    } catch (TTransportException e) {
      logger.warn("caught an exception", e);
      assertTrue(e.getCause() instanceof ReadTimeoutException);
    } finally {
      long timeout = System.currentTimeMillis() - startTime;
      logger.info("timeout {} ", timeout);
      try {
        assertTrue(timeout >= cfg.getNetworkTimeout());
      } finally {
        DummyTestServiceEngine.stop(engine);
      }
    }
  }

  @Test
  public void testClientsInMultipleThreadTimeout() throws Exception {
    logger.info("start test: testClientsInMultipleThreadTimeout");
    final int port1 = PortUtil.INSTANCE.allocatePort(55555);
    final int port2 = PortUtil.INSTANCE.allocatePort(port1 + 1);

    DummyTestServiceConfig cfg1 = DummyTestServiceConfig.create();
    cfg1.setServicePort(port1);
    cfg1.setLatency(5000);
    ThriftAppEngine engine1 = DummyTestServiceEngine.startEngine(cfg1);
    DummyTestServiceConfig cfg2 = DummyTestServiceConfig.create();
    cfg2.setLatency(4000);
    cfg2.setServicePort(port2);
    ThriftAppEngine engine2 = DummyTestServiceEngine.startEngine(cfg2);

    try {
      // let's create a dummy service's client
      final GenericThriftClientFactory<DummyTestService.Iface> factory = GenericThriftClientFactory
          .create(DummyTestService.Iface.class);

      class ClientRunnable implements Runnable {
        private long timeout;
        private int servicePort;

        public ClientRunnable(long timeout, int servicePort) {
          this.timeout = timeout;
          this.servicePort = servicePort;
        }

        @Override
        public void run() {
          long startTime = System.currentTimeMillis();
          try {
            EndPoint serviceEndPoint = EndPointParser
                .parseLocalEndPoint(servicePort, InetAddress.getLocalHost().getHostAddress());
            DummyTestService.Iface client = factory.generateSyncClient(serviceEndPoint, timeout);
            PingRequest request = new PingRequest(RequestIdBuilder.get());
            String response = client.ping(request);
            fail();
            logger.info("get response: " + response);
          } catch (TTransportException e) {
            long realTimeout = System.currentTimeMillis() - startTime;
            logger.info(realTimeout + " , configured timeout: " + timeout);
            assertTrue(realTimeout >= timeout);
          } catch (Exception e) {
            fail();
          }
        }
      }

      Thread thread1 = new Thread(new ClientRunnable(2000L, port1));
      Thread thread2 = new Thread(new ClientRunnable(2000L, port2));
      thread1.start();
      thread2.start();

      Thread.sleep(5000L);
      factory.close();
    } finally {
      DummyTestServiceEngine.stop(engine1);
      DummyTestServiceEngine.stop(engine2);
    }
  }

  @Test
  public void testAsyncClient() throws Exception {
    DummyTestServiceConfig cfg = DummyTestServiceConfig.create();
    ThriftAppEngine engine = DummyTestServiceEngine.startEngine(cfg);
    try {
      cfg.setReturnObject("ok");
      tryAsyncClient(cfg);
    } finally {
      DummyTestServiceEngine.stop(engine);
    }

    engine = DummyTestServiceEngine.startEngine(cfg);
    try {
      cfg.setReturnObject("result");
      tryAsyncClient(cfg);
    } finally {
      DummyTestServiceEngine.stop(engine);
    }
  }

  @Test
  public void testConcurrentClients() throws Exception {
    logger.info("start test: testConcurrentClients");
    final DummyTestServiceConfig cfg = DummyTestServiceConfig.create();
    cfg.setReturnObject("ok");
    cfg.setNetworkTimeout(5000);
    // To avoid overloading the service, set the number of threads in the service same as the number
    // of client.
    int numClients = 100;
    cfg.setMaxNumThreads(numClients);
    // make the worker thread smaller not to create too many threads. Otherwise, the system is going
    // to out of memory
    cfg.setNumWorkerThreads(3);
    ThriftAppEngine engine = null;
    try {
      engine = DummyTestServiceEngine.startEngine(cfg);

      // define a runnable
      final CountDownLatch latch = new CountDownLatch(numClients);
      final AtomicInteger responseOk = new AtomicInteger(0);
      for (int i = 0; i < numClients; i++) {
        new Thread(new Runnable() {
          @Override
          public void run() {
            try {
              if (trySyncClient(cfg)) {
                responseOk.incrementAndGet();
                logger.debug("response {} done", responseOk.get());
              } else {
                logger.debug("response {} not done", responseOk.get());
              }
            } catch (Exception e) {
              logger.error("caught an exception, response: {} ok", responseOk.get(), e);
            } finally {
              latch.countDown();
            }
          }
        }).start();
      }
      assertTrue(latch.await(60, TimeUnit.SECONDS));
      logger.info("wait is done");
      assertEquals(numClients, responseOk.get());
    } finally {
      DummyTestServiceEngine.stop(engine);
    }
  }

  @Test
  public void reuseInOnAnother() throws Exception {
    logger.info("start test: testSyncClient");
    ThriftAppEngine engine = null;
    DummyTestServiceConfig cfg = DummyTestServiceConfig.create();
    GenericThriftClientFactory<DummyTestService.Iface> factory = null;

    try {
      cfg.setReturnObject("ok");
      engine = DummyTestServiceEngine.startEngine(cfg);

      factory = GenericThriftClientFactory
          .create(DummyTestService.Iface.class, new TCompactProtocol.Factory(), 3, 5, 9);
      factory.setMaxNetworkFrameSize(cfg.getSendMaxFrameSize());
      EndPoint serviceEndPoint = EndPointParser
          .parseLocalEndPoint(cfg.getServicePort(), InetAddress.getLocalHost().getHostAddress());

      // first time
      DummyTestService.Iface client = factory
          .generateSyncClient(serviceEndPoint, cfg.getNetworkTimeout());
      PingRequest request = new PingRequest(RequestIdBuilder.get());
      request.setData((byte[]) null);
      String response = client.ping(request);
      assertEquals(cfg.getReturnObject(), response);

      // release the client
      factory.releaseSyncClient(client);

      // second time
      GenericThriftClientFactory<DummyTestService.Iface> tmpFactory = factory;
      Random random = new Random();
      int count = 10;
      CountDownLatch latch = new CountDownLatch(count);
      AtomicReference<Exception> error = new AtomicReference<>();
      for (int i = 0; i < count; i++) {
        Thread thread = new Thread() {
          public void run() {
            DummyTestService.Iface client = null;
            PingRequest request = new PingRequest(RequestIdBuilder.get());
            request.setData((byte[]) null);
            try {
              Thread.sleep(random.nextInt(20));
              client = tmpFactory.generateSyncClient(serviceEndPoint, cfg.getNetworkTimeout());
              String response = client.ping(request);
              assertEquals(cfg.getReturnObject(), response);
            } catch (Exception e) {
              logger.warn("caught an exception", e);
              error.set(e);
            }

            // always release the client
            logger.warn("exit the thread={}", Thread.currentThread().getId());
            tmpFactory.releaseSyncClient(client);
            latch.countDown();
          }
        };
        thread.start();
      }

      assertTrue(latch.await(20, TimeUnit.SECONDS));
      if (error.get() != null) {
        assertTrue(false);
      }
    } catch (Exception e) {
      logger.error("Caught an Exception", e);
      throw e;
    } finally {
      DummyTestServiceEngine.stop(engine);
      if (factory != null) {
        factory.close();
      }
    }
  }

  @Test
  public void resuseInOneThread() throws Exception {
    logger.info("start test: testSyncClient");
    ThriftAppEngine engine = null;
    DummyTestServiceConfig cfg = DummyTestServiceConfig.create();
    GenericThriftClientFactory<DummyTestService.Iface> factory = null;

    try {
      cfg.setReturnObject("ok");
      engine = DummyTestServiceEngine.startEngine(cfg);

      factory = GenericThriftClientFactory
          .create(DummyTestService.Iface.class, new TCompactProtocol.Factory(), 3, 5, 9);
      factory.setMaxNetworkFrameSize(cfg.getSendMaxFrameSize());
      EndPoint serviceEndPoint = EndPointParser
          .parseLocalEndPoint(cfg.getServicePort(), InetAddress.getLocalHost().getHostAddress());

      // first time
      DummyTestService.Iface client = factory
          .generateSyncClient(serviceEndPoint, cfg.getNetworkTimeout());
      PingRequest request = new PingRequest(RequestIdBuilder.get());
      request.setData((byte[]) null);
      String response = client.ping(request);
      assertEquals(cfg.getReturnObject(), response);

      // release the client
      factory.releaseSyncClient(client);

      // second time
      Random random = new Random();
      for (int i = 0; i < 10; i++) {
        client = factory.generateSyncClient(serviceEndPoint, cfg.getNetworkTimeout());
        request = new PingRequest(RequestIdBuilder.get());
        request.setData((byte[]) null);
        response = client.ping(request);
        assertEquals(cfg.getReturnObject(), response);
        // always release the client
        if (random.nextBoolean()) {
          factory.releaseSyncClient(client);
        }
      }
    } catch (Exception e) {
      logger.error("Caught an Exception", e);
      throw e;
    } finally {
      DummyTestServiceEngine.stop(engine);
      if (factory != null) {
        factory.close();
      }
    }
  }

  @Test
  public void testConcurrentAsyncClients() throws Exception {
    logger.info("start test: testConcurrentAsyncClients");
    Logger.getLogger("com.twitter.common.stats.Stats").setLevel(Level.OFF);

    final DummyTestServiceConfig cfg = DummyTestServiceConfig.create();
    // To avoid overloading the service, set the number of threads in the service same as the number
    // of client.
    int numClients = 100;
    cfg.setMaxNumThreads(numClients);
    cfg.setNetworkTimeout(5000);
    // make the worker thread smaller not to create too many threads. Otherwise, the system is going
    // to out of memory
    cfg.setNumWorkerThreads(3);
    cfg.setReturnObject("ok");
    ThriftAppEngine engine = DummyTestServiceEngine.startEngine(cfg);

    try {
      // define a runnable
      final CountDownLatch latch = new CountDownLatch(numClients);
      final AtomicInteger responseOk = new AtomicInteger(0);
      for (int i = 0; i < numClients; i++) {
        new Thread(new Runnable() {
          @Override
          public void run() {
            try {
              if (tryAsyncClient(cfg)) {
                responseOk.incrementAndGet();
              }
            } catch (Exception e) {
              logger.error("caught an exception", e);
            } finally {
              latch.countDown();
            }
          }
        }).start();
      }

      assertTrue(latch.await(60, TimeUnit.SECONDS));
      assertEquals(numClients, responseOk.get());
    } finally {
      DummyTestServiceEngine.stop(engine);
    }
  }

  // Are we sure onError is called?
  @Test
  public void testAsyncOnErrorCallback() throws Exception {
    DummyTestServiceConfig cfg = DummyTestServiceConfig.create();
    ThriftAppEngine engine = null;

    try {
      engine = DummyTestServiceEngine.startEngine(cfg);
      tryAsyncClient(cfg);
    } finally {
      DummyTestServiceEngine.stop(engine);
    }
  }

  @Test
  public void testCatchGenericThriftClientFactoryException() {
    // add this simple test just for exception print
    String message = "Time out";
    try {
      throw new GenericThriftClientFactoryException(message);
    } catch (GenericThriftClientFactoryException e) {
      // should use {}
      logger.error("1.0, caught an exception: {}", e.getClass().getSimpleName());
      logger.error("2.0, caught an exception: {}", e.getMessage());
      assertTrue(e instanceof GenericThriftClientFactoryException);
      assertTrue(e.getMessage().equals(message));
    }
  }

  private boolean tryAsyncClient(DummyTestServiceConfig cfg) throws Exception {
    return tryAsyncClient(cfg, null);
  }

  private boolean tryAsyncClient(final DummyTestServiceConfig cfg, byte[] data) throws Exception {
    // let's create a dummy service's client
    GenericThriftClientFactory<DummyTestService.AsyncIface> factory = null;
    try {
      factory = GenericThriftClientFactory
          .create(DummyTestService.AsyncIface.class, new TCompactProtocol.Factory(), 3, 5, 9);
      EndPoint serviceEndPoint = EndPointParser
          .parseLocalEndPoint(cfg.getServicePort(), InetAddress.getLocalHost().getHostAddress());
      DummyTestService.AsyncIface asynClient = factory
          .generateAsyncClient(serviceEndPoint, cfg.getNetworkTimeout(), 10000);
      final PingRequest request = new PingRequest(RequestIdBuilder.get());
      request.setData(data);
      final CountDownLatch latch = new CountDownLatch(1);
      final AtomicBoolean success = new AtomicBoolean(false);

      asynClient.ping(request, new AsyncMethodCallback<ping_call>() {
        @Override
        public void onComplete(ping_call arg0) {
          logger.info("onCompleted is called");
          try {
            assertEquals(cfg.getReturnObject(), arg0.getResult());
            success.set(true);
          } catch (TException e) {
            logger.error("caught an exception", e);
          }
          latch.countDown();
        }

        @Override
        public void onError(Exception arg0) {
          logger.warn("onError is called", arg0);
          latch.countDown();
        }
      });

      latch.await();
      return success.get();
    } finally {
      if (factory != null) {
        factory.close();
      }
    }
  }

  private boolean trySyncClient(DummyTestServiceConfig cfg) throws Exception {
    return trySyncClient(cfg, null);
  }

  private boolean trySyncClient(DummyTestServiceConfig cfg, byte[] data) throws Exception {
    GenericThriftClientFactory<DummyTestService.Iface> factory = null;
    try {
      factory = GenericThriftClientFactory
          .create(DummyTestService.Iface.class, new TCompactProtocol.Factory(), 3, 5, 9);
      factory.setMaxNetworkFrameSize(cfg.getSendMaxFrameSize());
      EndPoint serviceEndPoint = EndPointParser
          .parseLocalEndPoint(cfg.getServicePort(), InetAddress.getLocalHost().getHostAddress());
      DummyTestService.Iface client = factory
          .generateSyncClient(serviceEndPoint, cfg.getNetworkTimeout());
      PingRequest request = new PingRequest(RequestIdBuilder.get());
      request.setData(data);
      String response = client.ping(request);
      assertEquals(cfg.getReturnObject(), response);
      return true;
    } finally {
      if (factory != null) {
        factory.close();
      }
    }
  }

  private boolean tryDelegatableSyncClient(DummyTestServiceConfig cfg) throws Exception {
    return tryDelegatableSyncClient(cfg, null);
  }

  private boolean tryDelegatableSyncClient(DummyTestServiceConfig cfg, byte[] data)
      throws Exception {
    GenericThriftClientFactory<DummyTestService.Iface> factory = null;
    try {
      factory = GenericThriftClientFactory.create(DummyTestService.Iface.class);
      factory.setMaxNetworkFrameSize(10000);
      EndPoint serviceEndPoint = EndPointParser
          .parseLocalEndPoint(cfg.getServicePort(), InetAddress.getLocalHost().getHostAddress());
      DummyTestService.Iface client = factory
          .generateSyncClient(serviceEndPoint, cfg.getNetworkTimeout(), cfg.getNetworkTimeout(),
              true);
      PingRequest request = new PingRequest(RequestIdBuilder.get());
      request.setData(data);
      logger.debug("client class name: {}", client.getClass().getName());
      String response = client.ping(request);
      assertEquals(cfg.getReturnObject(), response);
      return true;
    } finally {
      if (factory != null) {
        factory.close();
      }
    }
  }

}
