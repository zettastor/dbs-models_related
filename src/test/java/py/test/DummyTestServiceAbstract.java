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
