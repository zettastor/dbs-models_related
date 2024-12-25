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

package py.drivercontainer.client;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.thrift.drivercontainer.service.DriverContainer;
import py.thrift.share.DriverLaunchFailedExceptionThrift;
import py.thrift.share.FailedToUmountDriverExceptionThrift;
import py.thrift.share.GetPerformanceFromPyMetricsResponseThrift;
import py.thrift.share.GetPerformanceParameterRequestThrift;
import py.thrift.share.GetPerformanceParameterResponseThrift;
import py.thrift.share.LaunchDriverRequestThrift;
import py.thrift.share.LaunchDriverResponseThrift;
import py.thrift.share.ReadPerformanceParameterFromFileExceptionThrift;
import py.thrift.share.ServiceHavingBeenShutdownThrift;
import py.thrift.share.UmountDriverRequestThrift;
import py.thrift.share.UmountDriverResponseThrift;
import py.thrift.share.VolumeHasNotBeenLaunchedExceptionThrift;

public class DriverContainerServiceBlockingClientWrapper {
  private static final Logger logger = LoggerFactory
      .getLogger(DriverContainerServiceBlockingClientWrapper.class);

  final DriverContainer.Iface delegate;

  public DriverContainerServiceBlockingClientWrapper(DriverContainer.Iface driverContainerClient) {
    this.delegate = driverContainerClient;
  }

  public DriverContainer.Iface getClient() {
    return delegate;
  }

  public void ping() throws TException {
    delegate.ping();
  }

  public LaunchDriverResponseThrift launchDriver(LaunchDriverRequestThrift request)
      throws DriverLaunchFailedExceptionThrift, TException {
    return delegate.launchDriver(request);
  }

  public UmountDriverResponseThrift umountDriver(UmountDriverRequestThrift request)
      throws ServiceHavingBeenShutdownThrift, FailedToUmountDriverExceptionThrift, TException {
    return delegate.umountDriver(request);
  }

  public GetPerformanceParameterResponseThrift pullPerformanceParameter(
      GetPerformanceParameterRequestThrift request)
      throws TException {
    return delegate.pullPerformanceParameter(request);
  }

  public GetPerformanceFromPyMetricsResponseThrift pullPerformanceFromPyMetrics(
      GetPerformanceParameterRequestThrift request) throws VolumeHasNotBeenLaunchedExceptionThrift,
      ReadPerformanceParameterFromFileExceptionThrift, TException {
    // TODO Auto-generated method stub
    return delegate.pullPerformanceFromPyMetrics(request);
  }

}
