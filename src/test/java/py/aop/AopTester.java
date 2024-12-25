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

package py.aop;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import org.apache.thrift.protocol.TProtocol;
import org.junit.Test;
import py.annotation.Retry;
import py.aop.annotation.Retryable;
import py.client.GenericProxyFactory;
import py.exception.LicenseException;
import py.test.TestBase;

public class AopTester extends TestBase {
  @Retry(times = 1, period = 1, when = {LicenseException.class, Exception.class})
  public void retryTester() {
  }

  @Test
  public void testRetry() {
    Retryable retryable = new Retryable();
    for (Method method : retryable.getClass().getDeclaredMethods()) {
      Retry retry = method.getAnnotation(Retry.class);
      if (retry == null) {
        continue;
      }

      logger.debug("{} {}", retry.times(), retry.period());
    }
  }

  @Test
  @Retry(times = 1, period = 1, when = {LicenseException.class, Exception.class})
  public void testRetry1() {
    Method[] methods = this.getClass().getDeclaredMethods();
    for (Method m : methods) {
      logger.debug("Methods are: {}", m.getName());

      Annotation[] annotations = m.getDeclaredAnnotations();
      logger.debug("Annotation size: {}", annotations.length);
      for (Annotation annotation : annotations) {
        logger.debug("Annotation are: {}", annotation);

        if (annotation.getClass().getName().equals("Retry")) {
          logger.debug("Retry annotation is {}", annotation);
        }
      }
    }
  }

  @Test
  public void testNoException() throws Exception {
    TestRetryService proxy = getProxy();
    proxy.noException();
    assertEquals(1, proxy.getInvokeCounterOfNoException());
  }

  @Test
  public void testSingleException() throws Exception {
    TestRetryService proxy = getProxy();
    Exception ex = null;
    try {
      proxy.singleException();
    } catch (Exception e) {
      logger.error("Caught an exception", e);
      ex = e;
    }
    assertNotNull(ex);
    assertEquals(3, proxy.getInvokeCounterOfSingleException());
  }

  @Test
  public void testMultiException() throws Exception {
    TestRetryService proxy = getProxy();
    Exception ex = null;
    try {
      proxy.multiException(new AopException1());
    } catch (Exception e) {
      logger.error("Caught an exception", e);
      ex = e;
    }
    assertNotNull(ex);
    assertEquals(3, proxy.getInvokeCounterOfMultiException1());
    assertEquals(0, proxy.getInvokeCounterOfMultiException2());
  }

  @Test
  public void testNoAnnotation() throws Exception {
    TestRetryService proxy = getProxy();
    Exception ex = null;
    try {
      proxy.noAnnotation();
    } catch (Exception e) {
      logger.error("Caught an exception", e);
      ex = e;
    }
    assertNull(ex);
    assertEquals(1, proxy.getInvokeCounterOfNoAnnotation());
  }

  @Test
  public void testSingleRetryAnnotation() throws Exception {
    TestRetryService proxy = getProxy();
    Exception ex = null;
    try {
      proxy.singleRetryAnnotation(new AopException1());
    } catch (Exception e) {
      logger.error("Caught an exception", e);
      ex = e;
    }
    assertNotNull(ex);
    assertEquals(3, proxy.getInvokeCounterOfSingleRetryAnnotation());

    proxy = getProxy();
    ex = null;
    try {
      proxy.singleRetryAnnotation(new AopException2());
    } catch (AopException2 e) {
      logger.error("Caught an exception", e);
      ex = e;
    }
    assertNull(ex);
    assertEquals(1, proxy.getInvokeCounterOfSingleRetryAnnotation());
  }

  @Test
  public void testSingleOtherAnnotation() throws Exception {
    TestRetryService proxy = getProxy();
    Exception ex = null;
    try {
      proxy.singleOtherAnnotation();
    } catch (Exception e) {
      logger.error("Caught an exception", e);
      ex = e;
    }
    assertNull(ex);
    assertEquals(1, proxy.getInvokeCounterOfSingleOtherAnnotation());
  }

  @Test
  public void testMultiAnnotationWithRetry() throws Exception {
    TestRetryService proxy = getProxy();
    Exception ex = null;
    try {
      proxy.multiAnnotationWithRetry();
    } catch (Exception e) {
      logger.error("Caught an exception", e);
      ex = e;
    }
    assertNotNull(ex);
    assertEquals(3, proxy.getInvokeCounterOfMultiAnnotationWithRetry());
  }

  @Test
  public void testRetrySuccess() throws Exception {
    TestRetryService proxy = getProxy();
    int retryTimesBeforeSuccess = 1;
    Exception ex = null;
    try {
      proxy.retrySuccess(retryTimesBeforeSuccess);
    } catch (Exception e) {
      logger.error("Caught an exception", e);
      ex = e;
    }
    assertNull(ex);
    assertEquals(retryTimesBeforeSuccess, proxy.getInvokeCounterOfRetrySuccess());
  }

  private TestRetryService getProxy() throws Exception {
    TestRetryService delegate = new TestRetryServiceImpl();
    GenericProxyFactory<TestRetryService> clientProxyFactory = new
        GenericProxyFactory<>(TestRetryService.class, delegate);
    TestRetryService proxy = null;
    try {
      Class<TProtocol>[] types = new Class[]{};
      Object[] intArgs = new Object[]{};
      proxy = clientProxyFactory.createJavassistDynamicProxy(types, intArgs);
    } catch (Exception e) {
      logger.error("Caught an exception", e);
      throw e;
    }
    return proxy;
  }
}
