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
