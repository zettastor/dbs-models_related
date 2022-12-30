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

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.common.RequestIdBuilder;
import py.common.tlsf.bytebuffer.manager.TlsfByteBufferManager;
import py.common.tlsf.bytebuffer.manager.TlsfByteBufferManagerFactory;
import py.exception.ArchiveTypeMismatchException;
import py.exception.ChecksumMismatchedException;
import py.exception.NotSupportedException;
import py.exception.StorageException;
import py.storage.Storage;

public abstract class AbstractArchiveBuilder {
  private static final Logger logger = LoggerFactory.getLogger(AbstractArchiveBuilder.class);

  protected String storagePath;
  protected boolean firstTimeStart;
  protected StorageType storageType;
  protected String devName;
  protected String currentUser;

  protected String serialNumber;
  protected boolean runInRealTime;
  protected boolean overwrite;
  protected ArchiveType archiveType;
  protected Long archiveId;

  protected Storage storage;
  protected boolean forceInitBuild;
  protected boolean justloadingExistArchive = true;
  protected String fileSystemPartitionName;

  public AbstractArchiveBuilder(ArchiveType archiveType, Storage storage) {
    this.archiveType = archiveType;
    this.storage = storage;
    archiveId = 0L;
  }

  public AbstractArchiveBuilder(String storagePath, boolean firstTimeStart, Long archveId,
      StorageType storageType, String devName,
      String currentUser, String serialNumber, boolean runInRealTime, boolean overwrite,
      ArchiveType archiveType,
      Storage storage, boolean forceInitBuild) {
    this.storagePath = storagePath;
    this.firstTimeStart = firstTimeStart;
    this.storageType = storageType;
    this.devName = devName;
    this.currentUser = currentUser;
    this.serialNumber = serialNumber;
    this.runInRealTime = runInRealTime;
    this.overwrite = overwrite;
    this.archiveType = archiveType;
    this.storage = storage;
    this.archiveId = archveId;
    this.forceInitBuild = forceInitBuild;
  }

  protected ArchiveMetadata generateArchiveMetadata() {
    ArchiveMetadata archiveMetadata = new ArchiveMetadata();
    archiveMetadata.setStorageType(storageType);
    archiveMetadata.setSerialNumber(serialNumber);
    archiveMetadata.setDeviceName(devName);
    archiveMetadata.setCreatedBy(currentUser);
    archiveMetadata.setCreatedTime(System.currentTimeMillis());
    archiveMetadata.setUpdatedTime(System.currentTimeMillis());
    archiveMetadata.setArchiveType(archiveType);
    archiveMetadata.setFileSystemPartitionName(fileSystemPartitionName);
    Long newArchiveId = 0L;
    if (archiveId.longValue() == 0L) {
      newArchiveId = RequestIdBuilder.get();
    } else {
      newArchiveId = archiveId;
    }
    archiveMetadata.setArchiveId(newArchiveId);
    return archiveMetadata;
  }

  public String getStoragePath() {
    return storagePath;
  }

  public void setStoragePath(String storagePath) {
    this.storagePath = storagePath;
  }

  public boolean isFirstTimeStart() {
    return firstTimeStart;
  }

  public void setFirstTimeStart(boolean firstTimeStart) {
    this.firstTimeStart = firstTimeStart;
  }

  public StorageType getStorageType() {
    return storageType;
  }

  public void setStorageType(StorageType storageType) {
    this.storageType = storageType;
  }

  public String getDevName() {
    return devName;
  }

  public void setDevName(String devName) {
    this.devName = devName;
  }

  public String getCurrentUser() {
    return currentUser;
  }

  public void setCurrentUser(String currentUser) {
    this.currentUser = currentUser;
  }

  public String getSerialNumber() {
    return serialNumber;
  }

  public void setSerialNumber(String serialNumber) {
    this.serialNumber = serialNumber;
  }

  public boolean isRunInRealTime() {
    return runInRealTime;
  }

  public void setRunInRealTime(boolean runInRealTime) {
    this.runInRealTime = runInRealTime;
  }

  public boolean isOverwrite() {
    return overwrite;
  }

  public void setOverwrite(boolean overwrite) {
    this.overwrite = overwrite;
  }

  public ArchiveType getArchiveType() {
    return archiveType;
  }

  public void setArchiveType(ArchiveType archiveType) {
    this.archiveType = archiveType;
  }

  public Long getArchiveId() {
    return archiveId;
  }

  public void setArchiveId(Long archiveId) {
    this.archiveId = archiveId;
  }

  public Storage getStorage() {
    return storage;
  }

  public void setStorage(Storage storage) {
    this.storage = storage;
  }

  public boolean isJustloadingExistArchive() {
    return justloadingExistArchive;
  }

  public void setJustloadingExistArchive(boolean justloadingExistArchive) {
    this.justloadingExistArchive = justloadingExistArchive;
  }

  public boolean isForceInitBuild() {
    return forceInitBuild;
  }

  public void setForceInitBuild(boolean forceInitBuild) {
    this.forceInitBuild = forceInitBuild;
  }

  public void setFileSystemPartitionName(String fileSystemPartitionName) {
    this.fileSystemPartitionName = fileSystemPartitionName;
  }

  public ArchiveMetadata loadArchiveMetadata() throws Exception {
    TlsfByteBufferManager tlsfByteBufferManager = TlsfByteBufferManagerFactory.instance();
    Validate.notNull(tlsfByteBufferManager);

    int metadataLength = ArchiveType.getArchiveHeaderLength();
    int metadataOffset = ArchiveType.getArchiveHeaderOffset();

    ByteBuffer buffer = tlsfByteBufferManager.blockingAllocate(metadataLength);
    try {
      storage.read(metadataOffset, buffer);
      buffer.clear();

      byte[] buf = new byte[metadataLength];
      buffer.get(buf, 0, buf.length);

      // magic number is always at the beginning of the buffer
      ByteBuffer byteBuffer = ByteBuffer
          .wrap(buf, Archive.MAGIC_NUMBER_OFFSET_IN_ARCHIVE,
              Archive.MAGIC_NUMBER_LENGTH_IN_ARCHIVE);
      long magic = byteBuffer.getLong();

      ArchiveType archiveTypeFromStorage = null;
      try {
        archiveTypeFromStorage = ArchiveType.findByMagicNumber(magic);
      } catch (NotSupportedException e) {
        logger.info("not support the magic number={}", magic);
      }

      if (archiveTypeFromStorage == null) {
        logger.warn("the storage is never formatted, magic={}, storage={}", magic, storage);
        return null;
      } else {
        logger.warn("the storage={} is formatted, type={}", storage, archiveTypeFromStorage);
      }

      // archive mismatch
      if (archiveType != null && archiveTypeFromStorage != archiveType) {
        // TODO: need add new exception
        throw new ArchiveTypeMismatchException(
            "archive type=" + archiveType + ", archive type from storage=" + archiveTypeFromStorage
                + ", storage=" + storage);
      }
      archiveType = archiveTypeFromStorage;

      // 8 is magic number length
      int metadataRealLen = getArchiveMetadataRealLength(buf,
          Archive.METADATA_LENGTH_OFFSET_IN_ARCHIVE);
      int metadataChecksumPos = Archive.getChecksumOffset(metadataRealLen);
      long expectedChecksum = getChecksum(buf, metadataChecksumPos);

      return instantiate(buf, Archive.getMetadataOffset(), metadataLength);
    } catch (Exception e) {
      logger.warn("caught an exception", e);
      throw e;
    } finally {
      tlsfByteBufferManager.release(buffer);
    }
  }

  public abstract Archive build()
      throws StorageException, IOException, ChecksumMismatchedException, Exception;

  protected abstract ArchiveMetadata instantiate(byte[] buffer, int offset, int length)
      throws ChecksumMismatchedException, IOException;

  private int getArchiveMetadataRealLength(byte[] buf, int offset) {
    ByteBuffer byteBuffer = ByteBuffer.wrap(buf, offset, Archive.METADATA_LENGTH_LENGTH_IN_ARCHIVE);
    int archiveMetadataLen = byteBuffer.getInt();
    if (archiveMetadataLen <= 0 || !Archive
        .checkArchiveMetadataLength(archiveType, archiveMetadataLen)) {
      throw new IllegalArgumentException(
          "bad archive metadata length: " + archiveMetadataLen + " exceed=" + archiveType
              .getArchiveHeaderLength());
    }
    return archiveMetadataLen;
  }

  public long getChecksum(byte[] buf, int offset) {
    ByteBuffer byteBuffer = ByteBuffer.wrap(buf, offset, Long.BYTES);
    long checksum = byteBuffer.getLong();
    logger.warn("readFromStorage: magic number of algorithm is {} and CheckSum is {}, length={}",
        Integer.toHexString((int) (checksum >> 32)), Integer.toHexString((int) (checksum)), offset);
    return checksum;
  }
}
