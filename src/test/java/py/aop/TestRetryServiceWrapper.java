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

import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestRetryServiceWrapper {
  private static final Logger logger = LoggerFactory.getLogger(TestRetryServiceWrapper.class);
  private TestRetryService delegate;

  public TestRetryServiceWrapper(TestRetryService delegate) {
    Validate.notNull(delegate);
    this.delegate = delegate;
  }

  public int testPerformance() throws Exception {
    // logger.debug("Invoke CounterServiceWrapper count");
    return delegate.testPerformance();
  }

  public TestRetryService getDelegate() {
    return delegate;
  }

  public void setDelegate(TestRetryService delegate) {
    this.delegate = delegate;
  }

}
