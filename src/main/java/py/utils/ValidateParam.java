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

package py.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.common.Constants;
import py.thrift.share.CreateStoragePoolRequestThrift;
import py.thrift.share.DeleteStoragePoolRequestThrift;
import py.thrift.share.InvalidInputExceptionThrift;
import py.thrift.share.ListStoragePoolCapacityRequestThrift;
import py.thrift.share.ListStoragePoolRequestThrift;
import py.thrift.share.RemoveArchiveFromStoragePoolRequestThrift;
import py.thrift.share.RemoveDatanodeFromDomainRequest;
import py.thrift.share.StoragePoolThrift;
import py.thrift.share.UpdateStoragePoolRequestThrift;

public class ValidateParam {
  private static final Logger logger = LoggerFactory.getLogger(ValidateParam.class);

  public static void validateCreateStoragePoolRequest(CreateStoragePoolRequestThrift request)
      throws InvalidInputExceptionThrift {
    // check params
    if (!request.isSetStoragePool()) {
      logger.error("Invalid create storage pool request:{}", request);
      throw new InvalidInputExceptionThrift();
    }
    StoragePoolThrift storagePoolFromRequest = request.getStoragePool();
    if (!storagePoolFromRequest.isSetDomainId() || !storagePoolFromRequest.isSetPoolId()
        || !storagePoolFromRequest.isSetPoolName() || !storagePoolFromRequest.isSetStrategy()) {
      logger.error("Invalid storage pool param:{}", storagePoolFromRequest);
      throw new InvalidInputExceptionThrift();
    }

    if (storagePoolFromRequest.isSetDescription()
        && storagePoolFromRequest.getDescription().length() > Constants.MAX_STORAGE_POOL_LENGTH) {
      logger
          .error("the storage pool description is max:{}", storagePoolFromRequest.getDescription());
      throw new InvalidInputExceptionThrift();
    }

  }

  public static void validateUpdateStoragePoolRequest(UpdateStoragePoolRequestThrift request)
      throws InvalidInputExceptionThrift {
    // check params
    if (!request.isSetStoragePool()) {
      logger.error("Invalid update storage pool request:{}", request);
      throw new InvalidInputExceptionThrift();
    }
    StoragePoolThrift storagePoolFromRequest = request.getStoragePool();
    if (!storagePoolFromRequest.isSetDomainId() || !storagePoolFromRequest.isSetPoolId()
        || !storagePoolFromRequest.isSetPoolName() || !storagePoolFromRequest.isSetStrategy()) {
      logger.error("Invalid storage pool param:{}", storagePoolFromRequest);
      throw new InvalidInputExceptionThrift();
    }
  }

  public static void validateDeleteStoragePoolRequest(DeleteStoragePoolRequestThrift request)
      throws InvalidInputExceptionThrift {
    // check params
    if (!request.isSetDomainId() || !request.isSetStoragePoolId()) {
      logger.error("Invalid storage pool param:{}", request);
      throw new InvalidInputExceptionThrift();
    }
  }

  public static void validateListStoragePoolRequest(ListStoragePoolRequestThrift request)
      throws InvalidInputExceptionThrift {
    // check params
    if (!request.isSetDomainId()) {
      logger.error("Invalid storage pool param:{}", request);
      throw new InvalidInputExceptionThrift();
    }
  }

  public static void validateListStoragePoolCapacityRequest(
      ListStoragePoolCapacityRequestThrift request)
      throws InvalidInputExceptionThrift {
    // check params
    if (!request.isSetDomainId()) {
      logger.error("Invalid storage pool param:{}", request);
      throw new InvalidInputExceptionThrift();
    }
  }

  public static void validateRemoveDatanodeFromDomainRequest(
      RemoveDatanodeFromDomainRequest request)
      throws InvalidInputExceptionThrift {
    // check params
    if (!request.isSetDomainId() || !request.isSetDatanodeInstanceId()) {
      logger.error("Invalid storage pool param:{}", request);
      throw new InvalidInputExceptionThrift();
    }
  }

  public static void validateRemoveArchiveFromStoragePoolRequest(
      RemoveArchiveFromStoragePoolRequestThrift request)
      throws InvalidInputExceptionThrift {
    // check params
    if (!request.isSetDatanodeInstanceId() || !request.isSetArchiveId() || !request.isSetDomainId()
        || !request.isSetStoragePoolId()) {
      logger.error("Invalid storage pool param:{}", request);
      throw new InvalidInputExceptionThrift();
    }
  }
}
