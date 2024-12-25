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

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.annotation.Retry;

public class TestRetryServiceImpl implements TestRetryService {
  private static final Logger logger = LoggerFactory.getLogger(TestRetryServiceImpl.class);
  private int invokeCounterOfPerformance = 0;
  private int invokeCounterOfNoExcetpion = 0;
  private int invokeCounterOfSingleException = 0;
  private int invokeCounterOfMultiException1 = 0;
  private int invokeCounterOfMultiException2 = 0;
  private int invokeCounterOfNoAnnotation = 0;
  private int invokeCounterOfSingleRetryAnnotation = 0;
  private int invokeCounterOfSingleOtherAnnotation = 0;
  private int invokeCounterOfMultiAnnotationWithRetry = 0;
  private int invokeCounterOfMultiAnnotationWithoutRetry = 0;
  private int invokeCounterOfRetrySuccess = 0;

  @Override
  public int testPerformance() throws Exception {
    return invokeCounterOfPerformance++;
  }

  @Retry
  @Override
  public void noException() {
    this.invokeCounterOfNoExcetpion++;
  }

  @Override
  @Retry(when = AopException1.class)
  public void singleException() throws Exception {
    invokeCounterOfSingleException++;
    try {
      throw new AopException1();
    } catch (Exception e) {
      logger.debug("exception type is {}", e.getClass().getName());
      throw e;
    }
  }

  @Override
  @Retry(when = {AopException1.class, AopException2.class})
  public void multiException(Exception exception) throws Exception {
    try {
      if (exception instanceof AopException1) {
        invokeCounterOfMultiException1++;
        throw exception;
      } else if (exception instanceof AopException2) {
        invokeCounterOfMultiException2++;
        throw exception;
      } else {
        throw new Exception();
      }
    } catch (Exception e) {
      logger.debug("exception type is {}", e.getClass().getName());
      throw e;
    }
  }

  // we must delete the '@Override' annotation here
  // @Override

  public void noAnnotation() throws Exception {
    invokeCounterOfNoAnnotation++;
    try {
      // do nothing
    } catch (Exception e) {
      logger.error("Caught an exception", e);
      throw e;
    }
  }

  // we must delete the '@Override' annotation here
  // @Override

  @Retry(when = AopException1.class)
  public void singleRetryAnnotation(Exception exception) throws Exception {
    invokeCounterOfSingleRetryAnnotation++;
    try {
      if (exception instanceof AopException1) {
        throw new AopException1();
      }
    } catch (Exception e) {
      logger.error("Caught an exception", e);
      throw e;
    }
  }

  @Override
  public void singleOtherAnnotation() throws Exception {
    invokeCounterOfSingleOtherAnnotation++;
    try {
      // do nothing
    } catch (Exception e) {
      logger.debug("exception type is {}", e.getClass().getName());
      throw e;
    }
  }

  @Override
  @Retry(when = AopException1.class)
  public void multiAnnotationWithRetry() throws Exception {
    invokeCounterOfMultiAnnotationWithRetry++;
    try {
      throw new AopException1();
    } catch (Exception e) {
      logger.debug("exception type is {}", e.getClass().getName());
      throw e;
    }
  }

  @Override
  @TestingAnnotation
  public void multiAnnotationWithOutRetry() throws Exception {
    invokeCounterOfMultiAnnotationWithRetry++;
    try {
      // do nothing
    } catch (Exception e) {
      logger.debug("exception type is {}", e.getClass().getName());
      throw e;
    }
  }

  @Override
  @Retry(when = AopException1.class)
  public void retrySuccess(int retryTimesBeforeSuccess) throws Exception {
    invokeCounterOfRetrySuccess++;
    try {
      if (invokeCounterOfRetrySuccess < retryTimesBeforeSuccess) {
        throw new AopException1();
      } else {
        //do not throw exception
      }
    } catch (Exception e) {
      logger.debug("exception type is {}", e.getClass().getName());
      throw e;
    }
  }

  @Override
  public int getInvokeCounterOfPerformance() {
    return invokeCounterOfPerformance;
  }

  @Override
  public int getInvokeCounterOfNoException() {
    return invokeCounterOfNoExcetpion;
  }

  @Override
  public int getInvokeCounterOfSingleException() {
    return invokeCounterOfSingleException;
  }

  @Override
  public int getInvokeCounterOfMultiException1() {
    return invokeCounterOfMultiException1;
  }

  @Override
  public int getInvokeCounterOfMultiException2() {
    return invokeCounterOfMultiException2;
  }

  @Override
  public int getInvokeCounterOfNoAnnotation() {
    return invokeCounterOfNoAnnotation;
  }

  @Override
  public int getInvokeCounterOfSingleRetryAnnotation() {
    return invokeCounterOfSingleRetryAnnotation;
  }

  @Override
  public int getInvokeCounterOfSingleOtherAnnotation() {
    return invokeCounterOfSingleOtherAnnotation;
  }

  @Override
  public int getInvokeCounterOfMultiAnnotationWithRetry() {
    return invokeCounterOfMultiAnnotationWithRetry;
  }

  @Override
  public int getInvokeCounterOfMultiAnnotationWithoutRetry() {
    return invokeCounterOfMultiAnnotationWithoutRetry;
  }

  @Override
  public int getInvokeCounterOfRetrySuccess() {
    return invokeCounterOfRetrySuccess;
  }

  @Target(ElementType.METHOD)
  @Retention(RetentionPolicy.SOURCE)
  private @interface TestingAnnotation {
  }

}
