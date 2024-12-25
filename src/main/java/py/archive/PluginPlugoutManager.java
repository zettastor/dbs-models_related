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

