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

package py.archive.segment.recurring;

import java.util.concurrent.Callable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * don't need to worry about synchronization of SegmentUnitTaskContext, because there is only one
 * thread touching it at any time.
 */
public abstract class SegmentUnitProcessor implements Callable<SegmentUnitProcessResult> {
  private static final Logger logger = LoggerFactory.getLogger(SegmentUnitProcessor.class);
  private final SegmentUnitTaskContext context;

  public SegmentUnitProcessor(SegmentUnitTaskContext context) {
    this.context = context;
  }

  // subclass needs to implement this function
  public abstract SegmentUnitProcessResult process();

  @Override
  public SegmentUnitProcessResult call() throws Exception {
    SegmentUnitProcessResult result = null;
    context.executionBegin(Thread.currentThread());
    try {
      result = process();
    } catch (Throwable t) {
      // normally process() function internally should handle
      // exceptions. If we get here, meaning that some unexpected
      // exception was thrown
      logger.error(
          "caught a mystery exception when process the segment unit context {} the context will be "
              + "resubmit",
          context, t);

      if (result == null) {
        result = new SegmentUnitProcessResult(context);
        result.setExecutionException(t);
        result.setExecutionSuccess(false);
      }
    } finally {
      context.executionEnd();
    }

    if (result == null) {
      logger.info(
          "the segment unit processor has returned a null result for the context {}. the context "
              + "will be resubmitted",
          context);
      result = new SegmentUnitProcessResult(context);
      result.setExecutionSuccess(false);
    }

    return result;
  }

  public SegmentUnitTaskContext getContext() {
    return context;
  }

}
