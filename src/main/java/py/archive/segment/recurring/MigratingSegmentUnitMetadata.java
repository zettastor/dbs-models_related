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
