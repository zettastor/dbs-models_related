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

public class SegmentUnitProcessResult {
  private final SegmentUnitTaskContext context;
  private Throwable executionException;
  private long delayToExecute;
  private boolean success;

  public SegmentUnitProcessResult(SegmentUnitTaskContext context) {
    this.context = context;
  }

  public Throwable getExecutionException() {
    return executionException;
  }

  public void setExecutionException(Throwable t) {
    this.executionException = t;
  }

  public boolean executionSuccess() {
    return success;
  }

  public void setExecutionSuccess(boolean success) {
    this.success = success;
  }

  public long getDelayToExecute() {
    return delayToExecute;
  }

  public void setDelayToExecute(long delayToExecute) {
    this.delayToExecute = delayToExecute;
  }

  public SegmentUnitTaskContext getContext() {
    return context;
  }

  @Override
  public String toString() {
    return "SegmentUnitProcessResult [executionException=" + executionException + ", context="
        + context
        + ", delayToExecute=" + delayToExecute + ", success=" + success + "]";
  }
}
