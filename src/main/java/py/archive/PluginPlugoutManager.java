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

import java.util.List;
import py.exception.ArchiveIsNotCleanedException;
import py.exception.ArchiveNotFoundException;
import py.exception.ArchiveStatusException;
import py.exception.ArchiveTypeNotSupportException;
import py.exception.InvalidInputException;
import py.exception.JnotifyAddListerException;
import py.exception.StorageException;
import py.thrift.share.ArchiveIsUsingExceptionThrift;

/**
 * the interface is defined for communicating with the hot plugin or plugout.
 *
 */
public interface PluginPlugoutManager {
  void plugin(Archive archive)
      throws ArchiveTypeNotSupportException, InvalidInputException, ArchiveIsUsingExceptionThrift,
      ArchiveStatusException, JnotifyAddListerException, ArchiveIsNotCleanedException,
      StorageException;

  Archive plugout(String devName, String serialNumber)
      throws ArchiveTypeNotSupportException, ArchiveNotFoundException, InterruptedException;

  /**
   * when a new archive is plugged in, maybe a new archive or a plugged out archive is plugged in,
   * so when a new archive is plugged in, the current archive will be influenced by the new archive.
   * When a plugged out archive is plugged in, the method will check and free the memory resource
   * related to the archive.
   */
  void hasPlugoutFinished(Archive archive) throws ArchiveIsNotCleanedException;

  List<Archive> getArchives();

  public Archive getArchive(long archiveId);
}

