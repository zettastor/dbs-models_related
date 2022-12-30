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