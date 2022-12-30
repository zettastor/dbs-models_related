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

import java.util.List;
import py.archive.segment.SegId;

public interface SegmentUnitTaskContextFactory {
  /**
   * generate a list of contexts related to the segment.
   */
  public List<SegmentUnitTaskContext> generateProcessingContext(SegId segId);

  /**
   * generate a list of new contexts according to the processing result of the previous round Note
   * that if a null or an empty list is returned, then no context will be added to the system.
   */
  public List<SegmentUnitTaskContext> generateProcessingContext(SegmentUnitProcessResult result);
}
