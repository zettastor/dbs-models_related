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
