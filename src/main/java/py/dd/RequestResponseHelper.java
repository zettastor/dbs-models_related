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

package py.dd;

import org.apache.commons.lang3.Validate;
import py.dd.common.ServiceMetadata;
import py.dd.common.ServiceStatus;
import py.thrift.share.ServiceMetadataThrift;
import py.thrift.share.ServiceStatusThrift;

public class RequestResponseHelper {
  public static ServiceStatusThrift convertServiceStatus(ServiceStatus serviceStatus) {
    Validate.notNull(serviceStatus);
    return Enum.valueOf(ServiceStatusThrift.class, serviceStatus.name());
  }

  public static ServiceStatus convertServiceStatus(ServiceStatusThrift serviceStatusThrift) {
    Validate.notNull(serviceStatusThrift);
    return Enum.valueOf(ServiceStatus.class, serviceStatusThrift.name());
  }

  public static ServiceMetadataThrift convertServiceMetadata(ServiceMetadata serviceMetadata) {
    ServiceMetadataThrift serviceMetadataThrift = new ServiceMetadataThrift();
    serviceMetadataThrift.setErrorCause(serviceMetadata.getErrorCause());
    serviceMetadataThrift.setServiceName(serviceMetadata.getServiceName());
    serviceMetadataThrift.setPid(serviceMetadata.getPid());
    serviceMetadataThrift.setPmpid(serviceMetadata.getPmpid());
    serviceMetadataThrift
        .setServiceStatus(convertServiceStatus(serviceMetadata.getServiceStatus()));
    serviceMetadataThrift.setVersion(serviceMetadata.getVersion());
    return serviceMetadataThrift;
  }

  public static ServiceMetadata convertServiceMetadata(
      ServiceMetadataThrift serviceMetadataThrift) {
    ServiceMetadata serviceMetadata = new ServiceMetadata();
    serviceMetadata.setErrorCause(serviceMetadataThrift.getErrorCause());
    serviceMetadata.setServiceName(serviceMetadataThrift.getServiceName());
    serviceMetadata.setPid(serviceMetadataThrift.getPid());
    serviceMetadata.setPmpid(serviceMetadataThrift.getPmpid());
    serviceMetadata
        .setServiceStatus(convertServiceStatus(serviceMetadataThrift.getServiceStatus()));
    serviceMetadata.setVersion(serviceMetadataThrift.getVersion());
    return serviceMetadata;
  }
}
