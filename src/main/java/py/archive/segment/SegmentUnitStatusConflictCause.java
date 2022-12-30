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

package py.archive.segment;

/**
 * Segment Unit status conflict type. Data Node will report their segment unit to information
 * center. But Segment unit 's status or something else may conflict with volume status. Information
 * center will throws exception with the cause
 */
public enum SegmentUnitStatusConflictCause {
  VolumeDeleted,                       // volume has been deleted
  VolumeRecycled,                      // volume is recycled after deleted
  StaleSnapshotVersion,                // snapshot version is lower than information center's
  RollbackToSnapshot,                  // volume is rolling back to snapshot
  VolumeExtendFailed,                  // volume extend failed
}
