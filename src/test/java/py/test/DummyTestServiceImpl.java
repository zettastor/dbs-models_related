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

import java.util.Random;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.junit.Ignore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.app.thrift.ThriftProcessorFactory;
import py.thrift.infocenter.service.ReserveVolumeRequest;
import py.thrift.infocenter.service.ReserveVolumeResponse;
import py.thrift.testing.service.DummyTestService;
import py.thrift.testing.service.PingRequest;
import py.thrift.testing.service.TestInternalErrorThrift;

@Ignore
public class DummyTestServiceImpl implements DummyTestService.Iface, ThriftProcessorFactory {
  private static final Logger logger = LoggerFactory.getLogger(DummyTestServiceImpl.class);
  private final DummyTestService.Processor<DummyTestService.Iface> processor;

  private Random random = new Random(System.currentTimeMillis());
  private DummyTestServiceConfig cfg;

  public DummyTestServiceImpl(DummyTestServiceConfig cfg) {
    this.processor = new DummyTestService.Processor<DummyTestService.Iface>(this);
    this.cfg = cfg;
  }

  @Override
  public void pingforcoodinator() throws TException {
  }

  @Override
  public TProcessor getProcessor() {
    return processor;
  }

  @Override
  public String ping(PingRequest request) throws TException {
    logger.debug("{}", request);
    // Deal With latency
    int letency = cfg.getLatency();
    if (letency != 0) {
      if (cfg.isRandomizeLatency()) {
        letency = random.nextInt(letency);
      }

      if (letency != 0) {
        try {
          logger.info("sleep: {}", letency);
          Thread.sleep(letency);
        } catch (InterruptedException e) {
          logger.error("interrupted ", e);
        }
      }
    }

    // Deal with return value
    if (cfg.getReturnObject() == null) {
      return null;
    }

    Object returnValue = cfg.getReturnObject();
    if (returnValue instanceof TException) {
      throw (TException) returnValue;
    } else {
      return (String) returnValue;
    }
  }

  @Override
  public ReserveVolumeResponse reserveVolume(ReserveVolumeRequest request)
      throws TestInternalErrorThrift,
      TException {
    // TODO Auto-generated method stub
    return null;
  }
}
