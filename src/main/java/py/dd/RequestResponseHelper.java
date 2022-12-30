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
