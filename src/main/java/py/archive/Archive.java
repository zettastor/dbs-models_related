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
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import py.exception.ArchiveNotExistException;
import py.exception.ArchiveStatusException;
import py.exception.StorageException;
import py.storage.Storage;

public abstract class Archive {
  public static final int MAGIC_NUMBER_OFFSET_IN_ARCHIVE = 0;
  public static final int MAGIC_NUMBER_LENGTH_IN_ARCHIVE = 8;
  public static final int METADATA_LENGTH_OFFSET_IN_ARCHIVE = 8;
  public static final int METADATA_LENGTH_LENGTH_IN_ARCHIVE = 4;
  public static final int CHECKSUM_LENGTH_IN_ARCHIVE = 8;

  protected volatile Storage storage;
  protected List<ArchiveStatusListener> archiveStatusListers = new CopyOnWriteArrayList<>();

  public Archive(Storage storage) {
    this.storage = storage;
  }

  public static boolean checkArchiveMetadataLength(ArchiveType archiveType, int metadataLength) {
    int maxLength = archiveType.getArchiveHeaderLength() - MAGIC_NUMBER_LENGTH_IN_ARCHIVE
        - METADATA_LENGTH_LENGTH_IN_ARCHIVE - CHECKSUM_LENGTH_IN_ARCHIVE;
    return metadataLength <= maxLength;
  }

  public static int getChecksumOffset(int metadataLen) {
    return Archive.MAGIC_NUMBER_LENGTH_IN_ARCHIVE + Archive.METADATA_LENGTH_LENGTH_IN_ARCHIVE
        + metadataLen;
  }

  public static int getMetadataOffset() {
    return MAGIC_NUMBER_LENGTH_IN_ARCHIVE + METADATA_LENGTH_LENGTH_IN_ARCHIVE;
  }

  public Storage getStorage() {
    return storage;
  }

  public void setStorage(Storage storage) {
    this.storage = storage;
  }

  //add listener must add archive manager first ,and then add infoReport lister,
  //otherwise ,archive status change may be error
  public abstract void addListener(ArchiveStatusListener listener);

  public abstract void clearArchiveStatusListener();

  public abstract ArchiveMetadata getArchiveMetadata();

  public abstract void persistMetadata() throws JsonProcessingException, StorageException;

  public abstract void setArchiveStatus(ArchiveStatus newArchiveStatus)
      throws ArchiveStatusException, ArchiveNotExistException;
}
