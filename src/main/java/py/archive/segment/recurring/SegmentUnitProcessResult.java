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
