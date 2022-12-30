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

import py.archive.AbstractSegmentUnitMetadata;
import py.archive.segment.SegId;
import py.storage.Storage;

public class MigratingSegmentUnitMetadata extends AbstractSegmentUnitMetadata {
  private long pageIndexerOffset;
  private volatile boolean discarded;

  public MigratingSegmentUnitMetadata(SegId segId, long metadataOffset, long dataOffset,
      Storage storage) {
    super(segId, metadataOffset, dataOffset, storage);
    discarded = false;
  }

  public static SegId getMigratingSegId(SegId originalSegId) {
    long volumeId = -originalSegId.getVolumeId().getId();
    return new SegId(volumeId, originalSegId.getIndex());
  }

  @Override
  public boolean isUnitReusable() {
    return discarded;
  }

  public void discard() {
    discarded = true;
  }

  public long getPageIndexerOffset() {
    return pageIndexerOffset;
  }

  public void setPageIndexerOffset(long pageIndexerOffset) {
    this.pageIndexerOffset = pageIndexerOffset;
  }
}
