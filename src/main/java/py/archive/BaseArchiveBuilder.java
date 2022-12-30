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
