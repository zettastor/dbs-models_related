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

package py.netty.datanode;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Test;
import py.netty.core.AbstractMethodCallback;
import py.netty.core.MethodCallback;
import py.netty.core.Protocol;
import py.netty.core.ProtocolFactory;
import py.netty.message.MethodType;
import py.netty.message.ProtocolBufProtocolFactory;
import py.proto.Broadcastlog;
import py.proto.Broadcastlog.PbAsyncSyncLogsBatchRequest;
import py.proto.Broadcastlog.PbAsyncSyncLogsBatchResponse;
import py.proto.Broadcastlog.PbBackwardSyncLogsRequest;
import py.proto.Broadcastlog.PbBackwardSyncLogsResponse;
import py.proto.Broadcastlog.PbCopyPageRequest;
import py.proto.Broadcastlog.PbCopyPageResponse;
import py.proto.Broadcastlog.PbMembership;
import py.proto.Broadcastlog.PbReadRequest;
import py.proto.Broadcastlog.PbWriteResponse;
import py.proto.Commitlog;
import py.test.TestBase;

public class ProtocolFactoryTest extends TestBase {
  @Test
  public void base() throws Exception {
    try {
      ProtocolFactory protocolFactory = ProtocolBufProtocolFactory.create(TestImpl.class);
      Protocol protocol = protocolFactory.getProtocol();
      assertNotNull(protocol.getMethod(MethodType.COPY.getValue()));
      assertNotNull(protocol.getMethod(MethodType.PING.getValue()));
      assertNotNull(protocol.getMethod(MethodType.WRITE.getValue()));
      assertNotNull(protocol.getMethod(MethodType.READ.getValue()));

      // get request type and response type
      final Method method = protocol.getMethod(MethodType.COPY.getValue());

      final TestImpl test = new TestImpl();
      AtomicBoolean hasCalled = new AtomicBoolean(false);
      final MethodCallback<Object> callback = new AbstractMethodCallback<Object>() {
        @Override
        public void complete(Object object) {
          hasCalled.set(true);
        }

        @Override
        public void fail(Exception exception) {
        }
      };

      PbCopyPageRequest.Builder builder = PbCopyPageRequest.newBuilder();
      builder.setRequestId(0L);
      builder.setSegIndex(0);
      builder.setSessionId(0L);
      builder.setVolumeId(0L);
      builder.setCopyPageUnitIndex(0);
      builder.setErrorCount(0);

      PbMembership.Builder builder1 = PbMembership.newBuilder();
      builder1.setEpoch(0);
      builder1.setGeneration(0);
      builder1.setPrimary(0);
      builder1.addAllSecondaries(new ArrayList<>());

      builder.setMembership(builder1.build());
      builder.build();
      Object[] args = new Object[]{builder.build(), callback};
      method.invoke(test, args);
      assertTrue(hasCalled.get());
    } catch (Exception e) {
      logger.warn("caught an exception", e);
    }
  }

  private class TestImpl extends AbstractAsyncService {
    public TestImpl() {
    }

    @Override
    public void copy(PyCopyPageRequest request, MethodCallback<PbCopyPageResponse> callback) {
      logger.info("echo copy page");
      callback.complete(null);
    }

    @Override
    public void ping(MethodCallback<Object> callback) {
    }

    @Override
    public void write(PyWriteRequest request, MethodCallback<PbWriteResponse> callback) {
    }

    @Override
    public void read(PbReadRequest request, MethodCallback<PyReadResponse> callback) {
    }

    @Override
    public void check(Broadcastlog.PbCheckRequest request,
        MethodCallback<Broadcastlog.PbCheckResponse> callback) {
    }

    @Override
    public void giveYouLogId(Broadcastlog.GiveYouLogIdRequest request,
        MethodCallback<Broadcastlog.GiveYouLogIdResponse> callback) {
    }

    @Override
    public void getMembership(Broadcastlog.PbGetMembershipRequest request,
        MethodCallback<Broadcastlog.PbGetMembershipResponse> callback) {
    }

    @Override
    public void addOrCommitLogs(Commitlog.PbCommitlogRequest request,
        MethodCallback<Commitlog.PbCommitlogResponse> callback) {
    }

    @Override
    public void discard(Broadcastlog.PbWriteRequest request,
        MethodCallback<PbWriteResponse> callback) {
    }

    @Override
    public void startOnlineMigration(Commitlog.PbStartOnlineMigrationRequest request,
        MethodCallback<Commitlog.PbStartOnlineMigrationResponse> callback) {
    }

    @Override
    public void syncLog(PbAsyncSyncLogsBatchRequest request,
        MethodCallback<PbAsyncSyncLogsBatchResponse> callback) {
    }

    @Override
    public void backwardSyncLog(PbBackwardSyncLogsRequest request,
        MethodCallback<PbBackwardSyncLogsResponse> callback) {
    }
  }
}
