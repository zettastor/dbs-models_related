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
