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

package py.archive.disklog;

import py.archive.Archive;
import py.exception.DiskBrokenException;
import py.exception.DiskDegradeException;
import py.exception.StorageException;

public interface DiskErrorLogManager {

  void putArchive(Archive archive) throws DiskDegradeException, DiskBrokenException;

  void recordError(Archive archive, StorageException error);

  void removeArchive(Archive archive);

  /*
   * for test purpose
   */
  long getErrorCount(Archive archive);

  void rawArchiveBroken(Archive rawArchive);
}
