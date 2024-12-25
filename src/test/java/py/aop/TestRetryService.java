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

public interface TestRetryService {
  int testPerformance() throws Exception;

  void noException();

  void singleException() throws Exception;

  void multiException(Exception exception) throws Exception;

  void noAnnotation() throws Exception;

  void singleRetryAnnotation(Exception exception) throws Exception;

  void singleOtherAnnotation() throws Exception;

  void multiAnnotationWithRetry() throws Exception;

  void multiAnnotationWithOutRetry() throws Exception;

  void retrySuccess(int retryTimesBeforeSuccess) throws Exception;

  int getInvokeCounterOfPerformance();

  int getInvokeCounterOfNoException();

  int getInvokeCounterOfSingleException();

  int getInvokeCounterOfMultiException1();

  int getInvokeCounterOfMultiException2();

  int getInvokeCounterOfNoAnnotation();

  int getInvokeCounterOfSingleRetryAnnotation();

  int getInvokeCounterOfSingleOtherAnnotation();

  int getInvokeCounterOfMultiAnnotationWithRetry();

  int getInvokeCounterOfMultiAnnotationWithoutRetry();

  int getInvokeCounterOfRetrySuccess();
}
