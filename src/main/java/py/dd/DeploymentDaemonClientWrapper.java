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

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.commons.lang3.Validate;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.common.PyService;
import py.common.RequestIdBuilder;
import py.dd.client.exception.ConfigurationNotFoundException;
import py.dd.client.exception.DriverIsAliveException;
import py.dd.client.exception.FailedToDeactivateServiceException;
import py.dd.client.exception.FailedToWipeoutException;
import py.dd.client.exception.ServiceIsBusyException;
import py.dd.client.exception.ServiceNotFoundException;
import py.dd.client.exception.ServiceStatusIsErrorException;
import py.dd.common.ServiceMetadata;
import py.thrift.deploymentdaemon.ActivateRequest;
import py.thrift.deploymentdaemon.BackupKeyRequest;
import py.thrift.deploymentdaemon.ChangeConfigurationRequest;
import py.thrift.deploymentdaemon.ConfigurationNotFoundExceptionThrift;
import py.thrift.deploymentdaemon.DeactivateRequest;
import py.thrift.deploymentdaemon.DeploymentDaemon;
import py.thrift.deploymentdaemon.DestroyRequest;
import py.thrift.deploymentdaemon.DriverIsAliveExceptionThrift;
import py.thrift.deploymentdaemon.FailedToWipeoutExceptionThrift;
import py.thrift.deploymentdaemon.GetStatusRequest;
import py.thrift.deploymentdaemon.GetStatusResponse;
import py.thrift.deploymentdaemon.GetUpgradeStatusRequest;
import py.thrift.deploymentdaemon.GetUpgradeStatusResponse;
import py.thrift.deploymentdaemon.PutTarRequest;
import py.thrift.deploymentdaemon.ServiceIsBusyExceptionThrift;
import py.thrift.deploymentdaemon.ServiceNotFoundExceptionThrift;
import py.thrift.deploymentdaemon.ServiceNotRunnableExceptionThrift;
import py.thrift.deploymentdaemon.ServiceStatusIsErrorExceptionThrift;
import py.thrift.deploymentdaemon.StartRequest;
import py.thrift.deploymentdaemon.UseBackupKeyRequest;
import py.thrift.deploymentdaemon.WipeoutRequest;
import py.thrift.share.UpgradeInfoThrift;

/**
 * A class includes some common used remote invoking of deployment daemon service interface.
 */
public class DeploymentDaemonClientWrapper {
  private static final Logger logger = LoggerFactory.getLogger(DeploymentDaemonClientWrapper.class);

  private int packageTransferSize;

  private List<String> params = new ArrayList<String>();

  private DeploymentDaemon.Iface delegate;

  public DeploymentDaemonClientWrapper(DeploymentDaemon.Iface delegate) {
    this.delegate = delegate;
  }

  public int getPackageTransferSize() {
    return packageTransferSize;
  }

  public void setPackageTransferSize(int packageTransferSize) {
    this.packageTransferSize = packageTransferSize;
  }

  public DeploymentDaemonClientWrapper attachParam(String param) {
    params.add(param);
    return this;
  }

  public void resetParams() {
    params.clear();
  }

  public DeploymentDaemon.Iface getClient() {
    return delegate;
  }

  /**
   * Ping deployment daemon to check if the service is ready to receive request.
   *
   */
  public boolean ping() {
    try {
      delegate.ping();
    } catch (TException e) {
      logger.error("Caught an exception when ping deployment daemon", e);
      return false;
    }

    return true;
  }

  public boolean transferPackage(String serviceName, String packageVersion,
      ByteBuffer packageBuffer) {
    PutTarRequest request = new PutTarRequest();
    request.setRequestId(RequestIdBuilder.get());
    request.setServiceName(serviceName);
    request.setServiceVersion(packageVersion);

    boolean append = false;
    int remaining = packageBuffer.limit();
    while (remaining > 0) {
      int readLen = (packageTransferSize < remaining) ? packageTransferSize : remaining;
      byte[] packageBytes = new byte[readLen];
      packageBuffer.get(packageBytes);
      // make sure the bytes length is equal to the read len
      request.setTarFile(packageBytes);
      request.setAppend(append);

      try {
        delegate.putTar(request);
      } catch (TException e) {
        logger
            .error("Caught an exception when transfer pacakge of service {} to remote", serviceName,
                e);
        break;
      }

      // append to the last byte stream if exist
      append = true;
      remaining -= readLen;
    }

    return remaining <= 0;
  }

  /**
   * For driverUpgrade coordinator need know timestamp or package name.
   */
  // Coordinator use timestamp for driver upgrade
  public boolean transferPackage(String serviceName, String packageVersion,
      ByteBuffer packageBuffer, String timestamp) {
    PutTarRequest request = new PutTarRequest();
    request.setRequestId(RequestIdBuilder.get());
    request.setServiceName(serviceName);
    request.setServiceVersion(packageVersion);
    if (serviceName.equals(PyService.COORDINATOR.getServiceName())) {
      request.setCoorTimestamp(timestamp);
    }

    boolean append = false;
    int remaining = packageBuffer.limit();
    while (remaining > 0) {
      int readLen = (packageTransferSize < remaining) ? packageTransferSize : remaining;
      byte[] packageBytes = new byte[readLen];
      packageBuffer.get(packageBytes);
      // make sure the bytes length is equal to the read len
      request.setTarFile(packageBytes);
      request.setAppend(append);

      try {
        delegate.putTar(request);
      } catch (TException e) {
        logger
            .error("Caught an exception when transfer pacakge of service {} to remote", serviceName,
                e);
        break;
      }

      // append to the last byte stream if exist
      append = true;
      remaining -= readLen;
    }

    return remaining <= 0;
  }

  /**
   * Transfer service package to remote machines as byte stream.
   */
  public boolean transferPackage(String serviceName, String packageVersion, Path packagePath) {
    boolean transferDone = false;

    PutTarRequest request = new PutTarRequest();
    request.setRequestId(RequestIdBuilder.get());
    request.setServiceName(serviceName);
    request.setServiceVersion(packageVersion);

    DataInputStream input = null;
    try {
      input = new DataInputStream(new FileInputStream(packagePath.toFile()));
    } catch (FileNotFoundException e1) {
      logger.error("No such package {}", packagePath.toString());
      return false;
    }

    int readLen = 0;
    boolean append = false;
    byte[] readBuffer = new byte[packageTransferSize];
    while (true) {
      try {
        readLen = input.read(readBuffer, 0, packageTransferSize);
      } catch (IOException e) {
        logger.error("Caught an exception when read bytes from package file {}",
            packagePath.toString(), e);
        break;
      }

      logger.debug("Read {} bytes from {}", readLen, packagePath);

      if (readLen <= 0) {
        logger.debug("No remain package bytes need to transfer to remote");
        transferDone = true;
        break;
      }

      // make sure the bytes length is equal to the read len
      byte[] packageBytes = new byte[readLen];
      for (int i = 0; i < readLen; i++) {
        packageBytes[i] = readBuffer[i];
      }
      request.setTarFile(packageBytes);
      request.setAppend(append);

      try {
        delegate.putTar(request);
      } catch (TException e) {
        logger
            .error("Caught an exception when transfer pacakge of service {} to remote", serviceName,
                e);
        break;
      }

      // append to the last byte stream if exist
      append = true;
    }

    if (input != null) {
      try {
        input.close();
      } catch (IOException e1) {
        logger.warn("Caught an exception when close input stream of file {}", packagePath, e1);
      }
    }

    return transferDone;
  }

  public boolean activate(String serviceName, String packageVersion)
      throws ServiceIsBusyException, ServiceStatusIsErrorException {
    ActivateRequest request = new ActivateRequest();
    request.setRequestId(RequestIdBuilder.get());
    request.setServiceName(serviceName);
    request.setServiceVersion(packageVersion);
    if (null != params) {
      request.setCmdParams(params);
    }

    try {
      delegate.activate(request);
    } catch (ServiceIsBusyExceptionThrift e) {
      throw new ServiceIsBusyException(e.getDetail());
    } catch (ServiceStatusIsErrorExceptionThrift e) {
      throw new ServiceStatusIsErrorException(e.getDetail());
    } catch (Exception e) {
      logger.error("Caught an exception when activate service {}", serviceName);
      return false;
    }

    return true;
  }

  public boolean deactivate(String serviceName, int servicePort)
      throws ServiceIsBusyException, FailedToDeactivateServiceException,
      ServiceStatusIsErrorException {
    DeactivateRequest request = new DeactivateRequest();
    request.setRequestId(RequestIdBuilder.get());
    request.setServiceName(serviceName);
    request.setServicePort(servicePort);

    try {
      delegate.deactivate(request);
    } catch (ServiceIsBusyExceptionThrift e) {
      throw new ServiceIsBusyException(e.getDetail());
    } catch (ServiceStatusIsErrorExceptionThrift e) {
      throw new ServiceStatusIsErrorException(e.getDetail());
    } catch (TException e) {
      logger.error("Caught an exception when deactivate service {}", serviceName);
      return false;
    }

    return true;
  }

  public boolean start(String serviceName)
      throws ServiceIsBusyException, ServiceStatusIsErrorException {
    StartRequest request = new StartRequest();
    request.setRequestId(RequestIdBuilder.get());
    request.setServiceName(serviceName);
    if (null != params) {
      request.setCmdParams(params);
    }

    try {
      delegate.start(request);
    } catch (ServiceIsBusyExceptionThrift e) {
      throw new ServiceIsBusyException(e.getDetail());
    } catch (ServiceStatusIsErrorExceptionThrift e) {
      throw new ServiceStatusIsErrorException(e.getDetail());
    } catch (Exception e) {
      logger.error("Caught an exception when start service {}", serviceName);
      return false;
    }

    return true;
  }

  public boolean wipeout(String serviceName, String coordinatorTimestamp, String fsserverTimestamp,
      String serverVersion) throws DriverIsAliveException {
    WipeoutRequest request = new WipeoutRequest();
    request.setRequestId(RequestIdBuilder.get());
    request.setServiceVersion(serverVersion);
    request.setServiceName(serviceName);
    if (serviceName.equals(PyService.DRIVERCONTAINER.getServiceName())
        || serviceName.equals(PyService.COORDINATOR.getServiceName())) {
      request.setCoorTimestamp(coordinatorTimestamp);
      request.setFsTimestamp(fsserverTimestamp);
    }
    logger.warn("coordinator timestamp :{},fs :{}", coordinatorTimestamp, fsserverTimestamp);

    try {
      delegate.wipeout(request);
    } catch (DriverIsAliveExceptionThrift e) {
      throw new DriverIsAliveException(e.getDetail());
    } catch (TException e) {
      logger.error("Caught an exception when wipeout service {}", serviceName, e);
      return false;
    }

    return true;
  }

  public boolean wipeout(String serviceName)
      throws DriverIsAliveException, FailedToWipeoutException {
    WipeoutRequest request = new WipeoutRequest();
    request.setRequestId(RequestIdBuilder.get());
    request.setServiceName(serviceName);
    try {
      delegate.wipeout(request);
    } catch (DriverIsAliveExceptionThrift e) {
      throw new DriverIsAliveException(e.getDetail());
    } catch (FailedToWipeoutExceptionThrift e) {
      throw new FailedToWipeoutException(e.getDetail());
    } catch (TException e) {
      logger.error("Caught an exception when wipeout service {}", serviceName, e);
      return false;
    }

    return true;
  }

  /**
   * The wipeout method use to wipeout all service in dd hosts,instead of service host infomation
   * from dd.properties file.
   */
  public boolean wipeout() throws DriverIsAliveException {
    WipeoutRequest request = new WipeoutRequest();
    request.setRequestId(RequestIdBuilder.get());
    //IgnoreConfig flag use to indicate this request is wipeout for dd hosts
    request.setIgnoreConfig(true);
    try {
      delegate.wipeout(request);
    } catch (DriverIsAliveExceptionThrift e) {
      throw new DriverIsAliveException(e.getDetail());
    } catch (TException e) {
      logger.error("Caught an exception when wipeout all service {}", e);
      return false;
    }
    return true;
  }

  public ServiceMetadata checkService(String serviceName)
      throws ServiceNotFoundException, ServiceNotRunnableExceptionThrift {
    GetStatusRequest request = new GetStatusRequest();
    request.setRequestId(RequestIdBuilder.get());
    request.setServiceName(serviceName);

    GetStatusResponse response;
    try {
      response = delegate.getStatus(request);
    } catch (ServiceNotFoundExceptionThrift e) {
      throw new ServiceNotFoundException(e.getDetail());
    } catch (ServiceNotRunnableExceptionThrift e) {
      throw e;
    } catch (Exception e) {
      logger.error("Caught an exception when check status of service {}", serviceName);
      return null;
    }

    Validate.notNull(response);

    ServiceMetadata serviceMetadata = new ServiceMetadata();
    serviceMetadata = RequestResponseHelper
        .convertServiceMetadata(response.getServiceMetadataThrift());
    return serviceMetadata;
  }

  public boolean checkUpgradeStatus(String serviceName)
      throws ServiceNotFoundException, ServiceNotRunnableExceptionThrift {
    boolean upgradeSuccess = false;

    GetUpgradeStatusRequest request = new GetUpgradeStatusRequest();
    request.setRequestId(RequestIdBuilder.get());
    request.setServiceName(serviceName);

    GetUpgradeStatusResponse response;
    try {
      response = delegate.getUpgradeStatus(request);
    } catch (ServiceNotFoundExceptionThrift e) {
      throw new ServiceNotFoundException(e.getDetail());
    } catch (ServiceNotRunnableExceptionThrift e) {
      throw e;
    } catch (Exception e) {
      logger.error("Caught an exception when check status of service {}", serviceName);
      return upgradeSuccess;
    }

    Validate.notNull(response);
    UpgradeInfoThrift upgradeInfo = response.getUpgradeInfoThrift();
    upgradeSuccess = upgradeInfo.isUpgrading();
    logger.debug("checkUpgradeStatus upgradeSuccess {}", upgradeSuccess);
    return upgradeSuccess;
  }

  public boolean destroy(String serviceName) throws ServiceIsBusyException {
    DestroyRequest request = new DestroyRequest(RequestIdBuilder.get(), serviceName);
    request.setServiceName(serviceName);
    try {
      delegate.destroy(request);
    } catch (ServiceIsBusyExceptionThrift e) {
      throw new ServiceIsBusyException(e.getDetail());
    } catch (TException e) {
      logger.error("Caught an exception when destroy service {},{}", serviceName, e);
      return false;
    }

    return true;
  }

  public boolean configure(String serviceName, String propsFileName, Properties properties)
      throws ServiceNotFoundException, ConfigurationNotFoundException {
    Map<String, String> propertiesTable = new HashMap<String, String>();
    for (Object key : properties.keySet()) {
      propertiesTable.put((String) key, (String) properties.get(key));
    }

    ChangeConfigurationRequest request = new ChangeConfigurationRequest();
    request.setRequestId(RequestIdBuilder.get());
    request.setServiceName(serviceName);
    request.setConfigFile(propsFileName);
    request.setChangingConfigurations(propertiesTable);
    request.setPreserve(true);

    try {
      delegate.changeConfiguration(request);
    } catch (ServiceNotFoundExceptionThrift e) {
      throw new ServiceNotFoundException(e.getDetail());
    } catch (ConfigurationNotFoundExceptionThrift e) {
      throw new ConfigurationNotFoundException(e.getDetail());
    } catch (TException e) {
      logger.error("Caught an exception when chang configuration of service {}", serviceName);
      return false;
    }

    return true;
  }

  public boolean backupKey(String serviceName, String serviceVersion)
      throws ServiceNotFoundException {
    BackupKeyRequest request = new BackupKeyRequest();
    request.setRequestId(RequestIdBuilder.get());
    request.setServiceName(serviceName);
    request.setServiceVersion(serviceVersion);

    try {
      delegate.backupKey(request);
    } catch (ServiceNotFoundExceptionThrift e) {
      throw new ServiceNotFoundException(e.getDetail());
    } catch (TException e) {
      logger.error("Caught an exception", e);
      return false;
    }

    return true;
  }

  public boolean useBackupKey(String serviceName, String serviceVersion)
      throws ServiceNotFoundException {
    UseBackupKeyRequest request = new UseBackupKeyRequest();
    request.setRequestId(RequestIdBuilder.get());
    request.setServiceName(serviceName);
    request.setServiceVersion(serviceVersion);

    try {
      delegate.useBackupKey(request);
    } catch (ServiceNotFoundExceptionThrift e) {
      throw new ServiceNotFoundException(e.getDetail());
    } catch (TException e) {
      logger.error("Caught an exception", e);
      return false;
    }

    return true;
  }
}
