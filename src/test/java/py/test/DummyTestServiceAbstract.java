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

package py.test;

import org.apache.thrift.TException;
import py.thrift.infocenter.service.ReserveVolumeRequest;
import py.thrift.infocenter.service.ReserveVolumeResponse;
import py.thrift.testing.service.DummyTestService;
import py.thrift.testing.service.PingRequest;
import py.thrift.testing.service.TestInternalErrorThrift;

public class DummyTestServiceAbstract implements DummyTestService.Iface {
  @Override
  public String ping(PingRequest request) throws TestInternalErrorThrift, TException {
    return new String("");
  }

  @Override
  public void pingforcoodinator() throws TException {
  }

  @Override
  public ReserveVolumeResponse reserveVolume(ReserveVolumeRequest request)
      throws TestInternalErrorThrift,
      TException {
    return new ReserveVolumeResponse();
  }
}
