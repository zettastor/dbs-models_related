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

package py.icshare.exception;

import py.archive.segment.SegmentUnitStatus;

public class InvalidSegmentStatusException extends Exception {
  private static final long serialVersionUID = 1L;

  public InvalidSegmentStatusException() {
    super();
  }

  public InvalidSegmentStatusException(SegmentUnitStatus newStatus,
      SegmentUnitStatus currentStatus) {
    super("current status " + currentStatus + " the new status " + newStatus);
  }

  public InvalidSegmentStatusException(String message) {
    super(message);
  }

  public InvalidSegmentStatusException(String message, Throwable cause) {
    super(message, cause);
  }

  public InvalidSegmentStatusException(Throwable cause) {
    super(cause);
  }
}
