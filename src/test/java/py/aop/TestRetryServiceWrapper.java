
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
