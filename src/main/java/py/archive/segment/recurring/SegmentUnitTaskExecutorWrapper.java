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

public class SegmentUnitTaskExecutorWrapper implements SegmentUnitTaskExecutor {
  private final SegmentUnitTaskExecutor[] executors;

  public SegmentUnitTaskExecutorWrapper(SegmentUnitTaskExecutor[] executors) {
    this.executors = executors;
  }

  private SegmentUnitTaskExecutor hashedExecutor(SegId segId) {
    return executors[(int) ((segId.getVolumeId().getId() + segId.getIndex()) % executors.length)];
  }

  @Override
  public void addSegmentUnit(SegId segId) throws RejectedExecutionException {
    hashedExecutor(segId).addSegmentUnit(segId);
  }

  @Override
  public void addSegmentUnit(SegId segId, boolean pauseProcessing)
      throws RejectedExecutionException {
    hashedExecutor(segId).addSegmentUnit(segId, pauseProcessing);
  }

  @Override
  public void removeSegmentUnit(SegId segId, SegmentUnitTaskCallback callback) {
    hashedExecutor(segId).removeSegmentUnit(segId, callback);
  }

  @Override
  public void pause(SegId segId) {
    hashedExecutor(segId).pause(segId);
  }

  @Override
  public void pause() {
    for (SegmentUnitTaskExecutor executor : executors) {
      executor.pause();
    }
  }

  @Override
  public boolean isAllContextsPaused(SegId segId) {
    return hashedExecutor(segId).isAllContextsPaused(segId);
  }

  @Override
  public void revive(SegId segId) {
    hashedExecutor(segId).revive(segId);
  }

  @Override
  public boolean isPaused(SegId segId) {
    return hashedExecutor(segId).isPaused(segId);
  }

  @Override
  public int getTaskCount() {
    int sum = 0;
    for (SegmentUnitTaskExecutor executor : executors) {
      sum += executor.getTaskCount();
    }
    return sum;
  }

  @Override
  public void shutdown() {
    for (SegmentUnitTaskExecutor executor : executors) {
      executor.shutdown();
    }
  }

  @Override
  public void restart() {
    for (SegmentUnitTaskExecutor executor : executors) {
      executor.restart();
    }
  }

  @Override
  public void start() {
    for (SegmentUnitTaskExecutor executor : executors) {
      executor.start();
    }
  }

  @Override
  public void pauseSegmentUnitProcessing(ContextKey particularContext) {
    hashedExecutor(particularContext.getSegId()).pauseSegmentUnitProcessing(particularContext);
  }
}
