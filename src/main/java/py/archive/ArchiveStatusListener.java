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
import py.exception.ArchiveIsNotCleanedException;
import py.exception.ArchiveNotExistException;
import py.exception.ArchiveStatusException;
import py.exception.StorageException;
import py.netty.exception.TimeoutException;

/**
 * Whoever implements this interface also needs to implement comparable interface, because
 * RawArchiveManager needs to compare whether two listeners are equal
 * this interface is listen the archive status, when archive status change, it call
 * the interface handleArchiveStatus do something.
 */

public interface ArchiveStatusListener {
  void becomeGood(Archive archive)
      throws ArchiveNotExistException, JsonProcessingException, StorageException,
      InterruptedException,
      TimeoutException, ArchiveStatusException;

  void becomeOfflining(Archive archive)
      throws ArchiveNotExistException, JsonProcessingException, StorageException,
      ArchiveStatusException;

  void becomeOfflined(Archive archive)
      throws ArchiveNotExistException, JsonProcessingException, StorageException,
      ArchiveStatusException,
      ArchiveIsNotCleanedException;

  void becomeEjected(Archive archive) throws ArchiveNotExistException, ArchiveStatusException;

  void becomeConfigMismatch(Archive archive)
      throws ArchiveNotExistException, ArchiveStatusException;

  void becomeDegrade(Archive archive) throws ArchiveNotExistException, ArchiveStatusException;

  void becomeInProperlyEjected(Archive archive)
      throws ArchiveNotExistException, ArchiveStatusException;

  void becomeBroken(Archive archive) throws ArchiveNotExistException, ArchiveStatusException;

}
