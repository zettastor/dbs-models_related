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

import py.exception.ArchiveNotExistException;

/**
 * when archive change status finished, notify this listener.
 */
public interface ArchiveStatusChangeFinishListener {
  void becomeGood(Archive archive, ArchiveStatus oldStatus)
      throws ArchiveNotExistException;

  void becomeOfflining(Archive archive, ArchiveStatus oldStatus) throws ArchiveNotExistException;

  void becomeOfflined(Archive archive, ArchiveStatus oldStatus)
      throws ArchiveNotExistException;

  void becomeEjected(Archive archive, ArchiveStatus oldStatus) throws ArchiveNotExistException;

  void becomeConfigMismatch(Archive archive, ArchiveStatus oldStatus)
      throws ArchiveNotExistException;

  void becomeDegrade(Archive archive, ArchiveStatus oldStatus) throws ArchiveNotExistException;

  void becomeInProperlyEjected(Archive archive, ArchiveStatus oldStatus)
      throws ArchiveNotExistException;

  void becomeBroken(Archive archive, ArchiveStatus oldStatus) throws ArchiveNotExistException;
}
