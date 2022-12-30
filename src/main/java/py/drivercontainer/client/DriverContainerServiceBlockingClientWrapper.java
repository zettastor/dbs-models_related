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
