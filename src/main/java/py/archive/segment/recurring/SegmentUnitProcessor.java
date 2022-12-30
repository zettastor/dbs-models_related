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