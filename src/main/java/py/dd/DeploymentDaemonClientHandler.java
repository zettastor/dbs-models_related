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
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.DeploymentDaemonClientFactory;
import py.common.PyService;
import py.common.RequestIdBuilder;
import py.dd.client.exception.FailedToActivateServiceException;
import py.dd.client.exception.FailedToDeactivateServiceException;
import py.dd.client.exception.FailedToStartServiceException;
import py.dd.client.exception.ServiceNotFoundException;
import py.dd.common.ServiceMetadata;
import py.dd.common.ServiceStatus;
import py.exception.GenericThriftClientFactoryException;
import py.exception.InternalErrorException;
import py.thrift.deploymentdaemon.ActivateRequest;
import py.thrift.deploymentdaemon.ActivateResponse;
import py.thrift.deploymentdaemon.ChangeConfigurationRequest;
import py.thrift.deploymentdaemon.DeactivateRequest;
import py.thrift.deploymentdaemon.DeactivateResponse;
import py.thrift.deploymentdaemon.DeploymentDaemon;
import py.thrift.deploymentdaemon.DestroyRequest;
import py.thrift.deploymentdaemon.FailedToActivateServiceExceptionThrift;
import py.thrift.deploymentdaemon.FailedToDeactivateServiceExceptionThrift;
import py.thrift.deploymentdaemon.FailedToDestroyServiceExceptionThrift;
import py.thrift.deploymentdaemon.FailedToRestartServiceExceptionThrift;
import py.thrift.deploymentdaemon.FailedToStartServiceExceptionThrift;
import py.thrift.deploymentdaemon.GetStatusRequest;
import py.thrift.deploymentdaemon.GetStatusResponse;
import py.thrift.deploymentdaemon.PrepareWorkspaceRequest;
import py.thrift.deploymentdaemon.PutTarRequest;
import py.thrift.deploymentdaemon.ServiceIsBusyExceptionThrift;
import py.thrift.deploymentdaemon.ServiceNotFoundExceptionThrift;
import py.thrift.deploymentdaemon.StartRequest;
import py.thrift.deploymentdaemon.StartResponse;
import py.thrift.deploymentdaemon.WipeoutRequest;
import py.thrift.share.ServiceStatusThrift;

/**
 * A deployment daemon class. When controlling instances of services from deployment daemon, you can
 * use this class to send request to server.
 */
public class DeploymentDaemonClientHandler {
  private static final Logger logger = LoggerFactory.getLogger(DeploymentDaemonClientHandler.class);

  private DeploymentDaemonClientFactory deploymentDaemonClientFactory;

  private String coordinatorTimeStamp;
  private String fsserverTimestamp;
  /**
   * Added by david for deploy a service with cmd params With cmdParams, this service will be
   * deployed with params within cmdParams.
   */
  private List<String> cmdParams = null;
  /*
   * This flag variable set default value false. To control action of changing configuration in
   * deployment daemon.
   */
  private boolean preserve = false;

  public String getFsserverTimestamp() {
    return fsserverTimestamp;
  }

  public void setFsserverTimestamp(String fsserverTimestamp) {
    this.fsserverTimestamp = fsserverTimestamp;
  }

  public String getCoordinatorTimeStamp() {
    return coordinatorTimeStamp;
  }

  public void setCoordinatorTimeStamp(String coordinatorTimeStamp) {
    this.coordinatorTimeStamp = coordinatorTimeStamp;
  }

  public List<String> getCmdParams() {
    return cmdParams;
  }

  /**
   * Added by david for set the params when startup for example: You can add a param "test" for
   * opening the JVM moniter port.
   */
  public void setCmdParams(List<String> cmdParams) {
    this.cmdParams = cmdParams;
  }

  public boolean isPreserve() {
    return preserve;
  }

  public void setPreserve(boolean preserve) {
    this.preserve = preserve;
  }

  public DeploymentDaemonClientFactory getDeploymentDaemonClientFactory() {
    return deploymentDaemonClientFactory;
  }

  public void setDeploymentDaemonClientFactory(
      DeploymentDaemonClientFactory deploymentDaemonClientFactory) {
    this.deploymentDaemonClientFactory = deploymentDaemonClientFactory;
  }

  /**
   * Put binary target file to remote machine.
   */
  public void put(String host, int port, String serviceName, String serviceVersion, String tarPath)
      throws FileNotFoundException, IOException, InternalErrorException,
      GenericThriftClientFactoryException,
      TException {
    // a request only send 10MB
    final int lenEachTime = 10 * 1024 * 1024;
    File tarFile;
    if (serviceName.equals(PyService.COORDINATOR.getServiceName())) {
      logger.warn("coortimeStamp in handler is :{}", coordinatorTimeStamp);
      tarFile = new File(
          tarPath + "/" + PyService.findValueByServiceName(serviceName).getServiceProjectKeyName()
              + "-" + serviceVersion + "-" + coordinatorTimeStamp + ".tar.gz");

    } else {
      tarFile = new File(
          tarPath + "/" + PyService.findValueByServiceName(serviceName).getServiceProjectKeyName()
              + "-" + serviceVersion + ".tar.gz");
    }

    boolean append = false;
    DeploymentDaemon.Iface client = null;
    List<PutTarRequest> tarRequests = new ArrayList<PutTarRequest>();
    byte[] readBuffer = new byte[lenEachTime];
    DataInputStream input = new DataInputStream(new FileInputStream(tarFile));
    int readLen = 0;
    while ((readLen = input.read(readBuffer, 0, lenEachTime)) > 0) {
      byte[] tarFileContent = new byte[readLen];
      for (int i = 0; i < readLen; i++) {
        tarFileContent[i] = readBuffer[i];
      }
      PutTarRequest request = new PutTarRequest();
      request.setRequestId(RequestIdBuilder.get());
      request.setServiceName(serviceName);
      request.setServiceVersion(serviceVersion);
      request.setTarFile(tarFileContent);
      request.setAppend(append);
      if (serviceName.equals(PyService.COORDINATOR.getServiceName())) {
        request.setCoorTimestamp(coordinatorTimeStamp);
      }
      tarRequests.add(request);
      if (!append) {
        append = true;
      }
    }

    for (PutTarRequest request : tarRequests) {
      client = deploymentDaemonClientFactory.build(host, port).getClient();
      client.putTar(request);
      client = null;
    }
    input.close();
  }

  /**
   * Activate service on remote machine if the service is not running. After this operation, old
   * service running path will be removed, and new service running path will be build. That means
   * old environment of service will disappear.
   */
  public boolean activate(String host, int port, String serviceName, String serviceVersion)
      throws InternalErrorException, FailedToActivateServiceException,
      GenericThriftClientFactoryException,
      FailedToActivateServiceExceptionThrift, ServiceIsBusyExceptionThrift, TException {
    DeploymentDaemon.Iface client = null;
    client = deploymentDaemonClientFactory.build(host, port).getClient();
    ActivateRequest request = new ActivateRequest();
    request.setRequestId(RequestIdBuilder.get());
    request.setServiceName(serviceName);
    request.setServiceVersion(serviceVersion);
    if (null != cmdParams) {
      request.setCmdParams(cmdParams);
    }
    logger.warn("active request:{}", request);
    ActivateResponse response = client.activate(request);
    if (response != null) {
      return true;
    }

    return false;
  }

  /**
   * Get service status running on remote machine. The status includes 'activate', 'activating',
   * 'deactivating' and 'deactive'.
   */
  public ServiceMetadata getStatus(String host, int port, String serviceName)
      throws InternalErrorException,
      ServiceNotFoundException, GenericThriftClientFactoryException, ServiceNotFoundExceptionThrift,
      TException {
    DeploymentDaemon.Iface client = null;
    client = deploymentDaemonClientFactory.build(host, port).getClient();
    GetStatusRequest request = new GetStatusRequest();
    request.setRequestId(RequestIdBuilder.get());
    request.setServiceName(serviceName);
    GetStatusResponse response = client.getStatus(request);
    if (response != null) {
      ServiceMetadata serviceMetadata = new ServiceMetadata();
      serviceMetadata.setServiceName(serviceName);
      serviceMetadata.setServiceStatus(
          ServiceStatus.valueOf(response.getServiceMetadataThrift().getServiceStatus().name()));
      serviceMetadata.setVersion(response.getServiceMetadataThrift().getVersion());
      return serviceMetadata;
    }

    return null;
  }

  /**
   * DD client to restart a specific service.
   */
  public boolean restart(String host, int port, String serviceName)
      throws GenericThriftClientFactoryException,
      FailedToRestartServiceExceptionThrift, ServiceIsBusyExceptionThrift, TException {
    // not call the restart interface of DeploymentDaemon because this interface will cause DD get
    // wrong status
    // after service reboot. Instead we first de_active the service then active the service

    // de_active service first:
    DeploymentDaemon.Iface client = null;
    client = deploymentDaemonClientFactory.build(host, port).getClient();
    DeactivateRequest deactiveRequest = new DeactivateRequest();
    deactiveRequest.setRequestId(RequestIdBuilder.get());
    deactiveRequest.setServiceName(serviceName);
    deactiveRequest.setSync(true);
    client.deactivate(deactiveRequest);

    // wait service become de_active or when active the service, it will throw
    // ServiceIsBusyExceptionThrift
    // exception
    GetStatusRequest getStatusRequest = new GetStatusRequest();
    getStatusRequest.setRequestId(RequestIdBuilder.get());
    getStatusRequest.setServiceName(serviceName);

    int interval = 2000;
    long times = 200000 / interval;
    while (times-- > 0) {
      try {
        GetStatusResponse statusResponse = client.getStatus(getStatusRequest);
        if (statusResponse.getServiceMetadataThrift().getServiceStatus()
            == ServiceStatusThrift.DEACTIVE) {
          break;
        }
        Thread.sleep(2000);
      } catch (Exception e) {
        logger.warn(" {} on {} is not running up", serviceName, host);
      }
    }

    if (times <= 0) {
      throw new FailedToRestartServiceExceptionThrift();
    }

    // active the service
    ActivateRequest activeRequest = new ActivateRequest();
    activeRequest.setRequestId(RequestIdBuilder.get());
    activeRequest.setServiceName(serviceName);
    if (null != cmdParams) {
      activeRequest.setCmdParams(cmdParams);
    }
    client.activate(activeRequest);

    return true;
  }

  /**
   * Stop service running on remote machine with synchronization which means service can do some
   * other thing after receive deactivate request and then stop.
   */
  public boolean deactivate(String host, int port, String serviceName, int servicePort,
      boolean sync)
      throws InternalErrorException, FailedToDeactivateServiceException,
      GenericThriftClientFactoryException, FailedToDeactivateServiceExceptionThrift,
      ServiceIsBusyExceptionThrift, TException {
    DeploymentDaemon.Iface client = null;
    client = deploymentDaemonClientFactory.build(host, port).getClient();
    DeactivateRequest request = new DeactivateRequest();
    request.setRequestId(RequestIdBuilder.get());
    request.setServiceName(serviceName);
    request.setSync(sync);
    request.setServicePort(servicePort);
    logger.warn("deactivate request:{}", request);
    DeactivateResponse response = client.deactivate(request);
    if (response != null) {
      return true;
    }

    return false;
  }

  /**
   * Start service running on remote machine with synchronization which means service can do some
   * other thing after receive deactivate request and then stop.
   */
  public boolean start(String host, int port, String serviceName)
      throws InternalErrorException, FailedToStartServiceException,
      GenericThriftClientFactoryException, FailedToStartServiceExceptionThrift,
      ServiceIsBusyExceptionThrift, TException {
    DeploymentDaemon.Iface client = null;
    client = deploymentDaemonClientFactory.build(host, port).getClient();
    StartRequest request = new StartRequest();
    request.setRequestId(RequestIdBuilder.get());
    request.setServiceName(serviceName);
    if (null != cmdParams) {
      request.setCmdParams(cmdParams);
    }
    // request.setSync(sync);
    StartResponse response = client.start(request);
    if (response != null) {
      return true;
    }

    return false;
  }

  /**
   * Change remote service configuration which configure in properties files.
   */
  public void changeConfiguration(String host, int port, String serviceName, String serviceVersion,
      String configFile,
      Map<String, String> changes)
      throws InternalErrorException, GenericThriftClientFactoryException, TException {
    DeploymentDaemon.Iface client = null;
    client = deploymentDaemonClientFactory.build(host, port).getClient();
    ChangeConfigurationRequest request = new ChangeConfigurationRequest();
    request.setRequestId(RequestIdBuilder.get());
    request.setServiceName(serviceName);
    request.setPreserve(preserve);
    request.setConfigFile(configFile);
    if (serviceVersion != null) {
      request.setServiceVersion(serviceVersion);
    }
    request.setChangingConfigurations(changes);

    if (serviceName.equals(PyService.COORDINATOR.getServiceName())) {
      request.setCoorTimestamp(coordinatorTimeStamp);
    }

    logger.info("request:{}, serviceName:{} host:{} configFile:{}", request, serviceName, host,
        configFile);
    client.changeConfiguration(request);
  }

  /**
   * Wipeout all services running on remote machine including stopping service, remove running
   * path.
   */
  public void wipeout(String host, int port)
      throws InternalErrorException, GenericThriftClientFactoryException, TException {
    DeploymentDaemon.Iface client = null;
    client = deploymentDaemonClientFactory.build(host, port).getClient();
    WipeoutRequest request = new WipeoutRequest(RequestIdBuilder.get());
    request.setCoorTimestamp(coordinatorTimeStamp);
    request.setFsTimestamp(fsserverTimestamp);
    client.wipeout(request);
  }

  /**
   * Force to stop services running on remote machine without synchronization.
   */
  public void destroy(String host, int port, String serviceName)
      throws GenericThriftClientFactoryException,
      FailedToDestroyServiceExceptionThrift, ServiceIsBusyExceptionThrift, TException {
    DeploymentDaemon.Iface client = null;
    client = deploymentDaemonClientFactory.build(host, port).getClient();
    DestroyRequest request = new DestroyRequest(RequestIdBuilder.get(), serviceName);
    request.setServiceName(serviceName);
    client.destroy(request);
  }

  public void prepareWorkspace(String host, int port, String serviceName)
      throws TException, GenericThriftClientFactoryException {
    logger.debug("servername in helper:{}", serviceName);
    DeploymentDaemon.Iface client = null;
    client = deploymentDaemonClientFactory.build(host, port).getClient();
    PrepareWorkspaceRequest request = new PrepareWorkspaceRequest(RequestIdBuilder.get(), null,
        serviceName);

    if (serviceName.equals(PyService.COORDINATOR.getServiceName())) {
      logger.debug("coordinator and timestamp in handler prepare is :{},{}", serviceName,
          coordinatorTimeStamp);
      request.setCoorTimestamp(coordinatorTimeStamp);
    }

    client.prepareWorkspace(request);
  }
}
