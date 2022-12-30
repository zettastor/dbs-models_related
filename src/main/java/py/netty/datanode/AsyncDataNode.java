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

import com.google.protobuf.AbstractMessageLite;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.connection.pool.PyConnection;
import py.netty.core.MethodCallback;
import py.netty.core.Protocol;
import py.netty.core.ProtocolFactory;
import py.netty.exception.DisconnectionException;
import py.netty.exception.InvalidProtocolException;
import py.netty.message.Header;
import py.netty.message.MethodType;
import py.netty.message.SendMessage;
import py.proto.Broadcastlog.GiveYouLogIdRequest;
import py.proto.Broadcastlog.GiveYouLogIdResponse;
import py.proto.Broadcastlog.PbAsyncSyncLogsBatchRequest;
import py.proto.Broadcastlog.PbAsyncSyncLogsBatchResponse;
import py.proto.Broadcastlog.PbBackwardSyncLogsRequest;
import py.proto.Broadcastlog.PbBackwardSyncLogsResponse;
import py.proto.Broadcastlog.PbCheckRequest;
import py.proto.Broadcastlog.PbCheckResponse;
import py.proto.Broadcastlog.PbCopyPageResponse;
import py.proto.Broadcastlog.PbGetMembershipRequest;
import py.proto.Broadcastlog.PbGetMembershipResponse;
import py.proto.Broadcastlog.PbReadRequest;
import py.proto.Broadcastlog.PbWriteRequest;
import py.proto.Broadcastlog.PbWriteResponse;
import py.proto.Commitlog;
import py.proto.Commitlog.PbStartOnlineMigrationRequest;
import py.proto.Commitlog.PbStartOnlineMigrationResponse;

public class AsyncDataNode {
  private static final Logger logger = LoggerFactory.getLogger(AsyncDataNode.class);

  public interface AsyncIface {
    public void ping(MethodCallback<Object> callback);

    public void write(PyWriteRequest request, MethodCallback<PbWriteResponse> callback);

    public void read(PbReadRequest request, MethodCallback<PyReadResponse> callback);

    public void copy(PyCopyPageRequest request, MethodCallback<PbCopyPageResponse> callback);

    public void check(PbCheckRequest request, MethodCallback<PbCheckResponse> callback);

    public void giveYouLogId(GiveYouLogIdRequest request,
        MethodCallback<GiveYouLogIdResponse> callback);

    public void getMembership(PbGetMembershipRequest request,
        MethodCallback<PbGetMembershipResponse> callback);

    public void addOrCommitLogs(Commitlog.PbCommitlogRequest request,
        MethodCallback<Commitlog.PbCommitlogResponse> callback);

    public void discard(PbWriteRequest request, MethodCallback<PbWriteResponse> callback);

    public void syncLog(PbAsyncSyncLogsBatchRequest request,
        MethodCallback<PbAsyncSyncLogsBatchResponse> callback);

    public void backwardSyncLog(PbBackwardSyncLogsRequest request,
        MethodCallback<PbBackwardSyncLogsResponse> callback);

    public void startOnlineMigration(PbStartOnlineMigrationRequest request,
        MethodCallback<PbStartOnlineMigrationResponse> callback);
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  public static class Client implements AsyncIface {
    private final PyConnection connection;
    private final Protocol protocol;

    public Client(PyConnection connection, Protocol protocol) {
      this.connection = connection;
      this.protocol = protocol;
    }

    @Override
    public void ping(MethodCallback<Object> callback) {
      Header header = generateHeader(MethodType.PING, 0, 0);
      send(header, null, callback, null);
    }

    @Override
    public void write(PyWriteRequest request, MethodCallback<PbWriteResponse> callback) {
      Header header = generateHeader(MethodType.WRITE, request.getMetadata().getSerializedSize(),
          request.getDataLength());
      send(header, request, callback, request.getData(), false);
    }

    @Override
    public void read(PbReadRequest request, MethodCallback<PyReadResponse> callback) {
      Header header = generateHeader(MethodType.READ, request.getSerializedSize(), 0);
      send(header, request, callback, null);
    }

    @Override
    public void copy(PyCopyPageRequest request, MethodCallback<PbCopyPageResponse> callback) {
      Header header = generateHeader(MethodType.COPY, request.getMetadata().getSerializedSize(),
          request.getDataLength());
      send(header, request, callback, request.getData(), false);
    }

    public void check(PbCheckRequest request, MethodCallback<PbCheckResponse> callback) {
      Header header = generateHeader(MethodType.CHECK, request.getSerializedSize(), 0);
      send(header, request, callback, null);
    }

    @Override
    public void giveYouLogId(GiveYouLogIdRequest request,
        MethodCallback<GiveYouLogIdResponse> callback) {
      Header header = generateHeader(MethodType.GIVEYOULOGID, request.getSerializedSize(), 0);
      send(header, request, callback, null);
    }

    @Override
    public void getMembership(PbGetMembershipRequest request,
        MethodCallback<PbGetMembershipResponse> callback) {
      Header header = generateHeader(MethodType.GETMEMBERSHIP, request.getSerializedSize(), 0);
      send(header, request, callback, null);
    }

    @Override
    public void addOrCommitLogs(Commitlog.PbCommitlogRequest request,
        MethodCallback<Commitlog.PbCommitlogResponse> callback) {
      Header header = generateHeader(MethodType.ADDORCOMMITLOGS, request.getSerializedSize(), 0);
      send(header, request, callback, null);
    }

    @Override
    public void discard(PbWriteRequest request, MethodCallback<PbWriteResponse> callback) {
      Header header = generateHeader(MethodType.DISCARD, request.getSerializedSize(), 0);
      send(header, request, callback, null);
    }

    @Override
    public void startOnlineMigration(PbStartOnlineMigrationRequest request,
        MethodCallback<PbStartOnlineMigrationResponse> callback) {
      Header header = generateHeader(MethodType.STARTONLINEMIGRATION, request.getSerializedSize(),
          0);
      send(header, request, callback, null);
    }

    @Override
    public void syncLog(PbAsyncSyncLogsBatchRequest request,
        MethodCallback<PbAsyncSyncLogsBatchResponse> callback) {
      Header header = generateHeader(MethodType.SYNCLOG, request.getSerializedSize(), 0);
      send(header, request, callback, null);
    }

    @Override
    public void backwardSyncLog(PbBackwardSyncLogsRequest request,
        MethodCallback<PbBackwardSyncLogsResponse> callback) {
      Header header = generateHeader(MethodType.BACKWARDSYNCLOG, request.getSerializedSize(), 0);
      send(header, request, callback, null);
    }

    private Header generateHeader(MethodType methodType, int metadataLength, int dataLength) {
      return new Header((byte) methodType.getValue(), metadataLength, dataLength,
          ProtocolFactory.getRequestId());
    }

    public void send(Header header, AbstractMessageLite request, MethodCallback callback,
        ByteBuf data) {
      send(header, request, callback, data, true);
    }

    // bFlush is deprecated. send() will only call channel.write. The netty system is responsible
    // for flushing data

    public void send(Header header, AbstractMessageLite request, MethodCallback callback,
        ByteBuf data,
        boolean bflush) {
      ByteBuf metadata = null;
      try {
        metadata = protocol.encodeRequest(header, request);
      } catch (InvalidProtocolException e) {
        logger.warn("caught an exception", e);
        if (metadata != null) {
          metadata.release();
        }
        callback.fail(e);
        return;
      }

      Validate.notNull(metadata);
      ByteBuf dataForWholeMessage = null;
      try {
        if (data != null) {
          dataForWholeMessage = Unpooled.wrappedBuffer(metadata, data);
        } else {
          dataForWholeMessage = metadata;
        }
        Validate.notNull(dataForWholeMessage);

        AtomicBoolean isResponse = new AtomicBoolean(false);
        MethodCallback<AbstractMessageLite> callbackWrapper =
            new MethodCallback<AbstractMessageLite>() {
              @Override
              public void complete(AbstractMessageLite object) {
                if (isResponse.compareAndSet(false, true)) {
                  callback.complete(object);
                } else {
                  logger.warn("callback already been invoked. Request:{}, isResponse:{}",
                      header.getRequestId(), isResponse);
                }
              }

              @Override
              public void fail(Exception e) {
                if (isResponse.compareAndSet(false, true)) {
                  callback.fail(e);
                } else {
                  logger.warn("callback already been invoked. Request:{}, isResponse:{}.",
                      header.getRequestId(), isResponse, e);
                }
              }

              @Override
              public ByteBufAllocator getAllocator() {
                return callback.getAllocator();
              }
            };
        connection.write(new SendMessage(header, dataForWholeMessage, callbackWrapper));
      } catch (Exception e) {
        logger.error("caught an exception", e);
        if (dataForWholeMessage != null) {
          dataForWholeMessage.release();
        }
        callback.fail(new DisconnectionException(e.getMessage()));
      }

    }
  }
}
