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
