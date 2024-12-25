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

package py.deployment.daemon;

import static org.mockito.ArgumentMatchers.argThat;

import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.dd.DeploymentDaemonClientWrapper;
import py.test.TestBase;
import py.thrift.deploymentdaemon.DeploymentDaemon;
import py.thrift.deploymentdaemon.PutTarRequest;

/**
 * A class includes some test for deployment daemon client wrapper.
 */
public class DeploymentDaemonClientWrapperTest extends TestBase {
  private static final Logger logger = LoggerFactory
      .getLogger(DeploymentDaemonClientWrapperTest.class);

  @Mock
  private DeploymentDaemon.Iface delegate;

  private DeploymentDaemonClientWrapper ddClient;

  @Before
  public void init() throws Exception {
    super.init();

    ddClient = new DeploymentDaemonClientWrapper(delegate);
  }

  @Test
  public void testTransferingServicePackage() throws Exception {
    Path packagePath = Paths.get("/tmp/transferServicePackage");
    if (packagePath.toFile().exists()) {
      packagePath.toFile().delete();
    }

    final byte[] byteBuf = new byte[1024];
    for (int i = 0; i < byteBuf.length; i++) {
      byteBuf[i] = (byte) (i % Byte.MAX_VALUE);
    }
    FileOutputStream outStream = new FileOutputStream(packagePath.toFile());
    outStream.write(byteBuf);
    outStream.write(byteBuf);
    outStream.close();

    ddClient.setPackageTransferSize(byteBuf.length);
    ddClient.transferPackage("", "", packagePath);

    class IsRequest implements ArgumentMatcher<PutTarRequest> {
      public boolean matches(PutTarRequest request) {
        if (request == null) {
          return false;
        }

        if (byteBuf.length != request.getTarFile().length) {
          logger.info("Char buf length is not same, write char buf is {}, but read char buf is {}",
              byteBuf.length, request.getTarFile().length);
          return false;
        }
        for (int i = 0; i < request.getTarFile().length; i++) {
          if (request.getTarFile()[i] != byteBuf[i]) {
            logger.info("Char at {} is different, write char is {}, but read char is {}", i,
                byteBuf[i],
                request.getTarFile()[i]);
            return false;
          }
        }
        return true;
      }
    }

    Mockito.verify(delegate, Mockito.times(2))
        .putTar(argThat(new IsRequest()));

    ddClient.transferPackage("", "", ByteBuffer.wrap(byteBuf));
    Mockito.verify(delegate, Mockito.times(3))
        .putTar(argThat(new IsRequest()));
  }
}
