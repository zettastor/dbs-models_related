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

package py.archive;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.io.IOException;
import py.exception.ArchiveStatusException;
import py.exception.ChecksumMismatchedException;
import py.exception.StorageException;
import py.storage.Storage;

public class BaseArchiveBuilder extends AbstractArchiveBuilder {
  public BaseArchiveBuilder(ArchiveType archiveType, Storage storage) {
    super(archiveType, storage);
  }

  @Override
  public Archive build()
      throws StorageException, IOException, ChecksumMismatchedException, Exception {
    ArchiveMetadata archiveMetadata = super.loadArchiveMetadata();
    return new Archive(storage) {
      @Override
      public void addListener(ArchiveStatusListener listener) {
        return;
      }

      @Override
      public void clearArchiveStatusListener() {
        return;
      }

      @Override
      public ArchiveMetadata getArchiveMetadata() {
        return archiveMetadata;
      }

      @Override
      public void persistMetadata() throws JsonProcessingException, StorageException {
        throw new RuntimeException(
            "this is a base builder for storage=" + storage + " type=" + archiveType);
      }

      @Override
      public void setArchiveStatus(ArchiveStatus newArchiveStatus) throws ArchiveStatusException {
        return;
      }

    };
  }

  @Override
  protected ArchiveMetadata instantiate(byte[] buffer, int offset, int length)
      throws ChecksumMismatchedException, IOException {
    ArchiveMetadata metadata = generateArchiveMetadata();
    metadata.setStatus(ArchiveStatus.GOOD);
    return metadata;
  }
}
