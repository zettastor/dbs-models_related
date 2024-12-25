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

import java.util.concurrent.RejectedExecutionException;
import py.archive.segment.SegId;

/**
 * The executor that executes tasks related to segment units.
 */
public interface SegmentUnitTaskExecutor {
  /**
   * submit a task associated with a segId and with a specified context and certain delay.
   */

  public void addSegmentUnit(SegId segId) throws RejectedExecutionException;

  public void addSegmentUnit(SegId segId, boolean pauseProcessing)
      throws RejectedExecutionException;

  public void removeSegmentUnit(SegId segId, SegmentUnitTaskCallback callback);

  // When the status of a segment unit has been changed, maybe we need pause the segment unit to
  // stop driving PCL and PPL
  public void pause(SegId segId);

  /* This function can result in pausing the engine completely to process any segment units */
  void pause();

  // when pausing some segment unit, we should wait all contexts to being paused.  add junit test
  public boolean isAllContextsPaused(SegId segId);

  // when a segment unit runs into normal status, maybe we need revive the segment unit to drive
  // PCL and PPL
  public void revive(SegId segId);

  // check if the segment unit has paused
  public boolean isPaused(SegId segId);

  /**
   * return the approximate task counts.
   */
  public int getTaskCount();

  /* Invoked only when the whole system is shutdown. Otherwise other threads might be interrupted */
  // we should find out why AsyncClientManager.select() is interrupted when this function is invoked
  void shutdown();

  /*
   * restart the engine to pull tasks from the queue and execute them. If the engine was not paused
   * previously, the function won't don't anything and it can be called multiple times
   */
  void restart();

  /* start the engin and exact the same thing as restart() */
  void start();

  /**
   * Compared with pauseSegmentUnitProcessing(SegId segId), this API allows to pause a special
   * context type. For instance, it allows to pause PCLDriver only, without impacting PLALDriver and
   * PPLDriver.
   */
  void pauseSegmentUnitProcessing(ContextKey particularContext);
}
