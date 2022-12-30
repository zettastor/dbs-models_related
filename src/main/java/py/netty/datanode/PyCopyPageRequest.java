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

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.GeneratedMessage;
import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import org.apache.commons.lang.Validate;
import py.archive.ArchiveOptions;
import py.netty.message.Header;
import py.netty.message.Message;
import py.netty.message.MessageCarryDataInterface;
import py.proto.Broadcastlog.PbCopyPageRequest;

public class PyCopyPageRequest extends GeneratedMessage implements MessageCarryDataInterface {
  private static int pageSize;
  private static Method parseFrom;

  static {
    try {
      parseFrom = PbCopyPageRequest.class.getMethod("parseFrom", CodedInputStream.class);
    } catch (NoSuchMethodException | SecurityException e) {
      throw new RuntimeException("can init class PYCopyPageRequest");
    }
    pageSize = (int) ArchiveOptions.PAGE_SIZE;
  }

  private final PbCopyPageRequest metadata;
  private final ByteBuf data;

  public PyCopyPageRequest(PbCopyPageRequest metadata, ByteBuf data) {
    this.metadata = metadata;
    this.data = data;
  }

  public static PyCopyPageRequest parseFrom(Message msg, CodedInputStream inputStream)
      throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {
    Header header = msg.getHeader();
    ByteBuf body = msg.getBuffer();

    PbCopyPageRequest object = (PbCopyPageRequest) parseFrom.invoke(null, inputStream);
    if (header.getDataLength() > 0) {
      Validate.isTrue(header.getDataLength() == body.readableBytes());
      return new PyCopyPageRequest(object, body);
    } else {
      return new PyCopyPageRequest(object, null);
    }
  }

  public PyCopyPageRequest clone() {
    return new PyCopyPageRequest(metadata, data.retain());
  }

  public ByteBuf getRequestUnitData(int index) {
    return data.slice(data.readerIndex() + index * pageSize, pageSize);
  }

  public PbCopyPageRequest getMetadata() {
    return metadata;
  }

  @Override
  public ByteBuf getData() {
    return data;
  }

  @Override
  public int getDataLength() {
    return data == null ? 0 : data.readableBytes();
  }

  public int getSerializedSize() {
    if (metadata == null) {
      return 0;
    }
    return metadata.getSerializedSize();
  }

  @Override
  public void writeTo(final CodedOutputStream output) throws IOException {
    metadata.writeTo(output);
  }

  @Override
  public com.google.protobuf.Message.Builder newBuilderForType() {
    return null;
  }

  @Override
  protected com.google.protobuf.Message.Builder newBuilderForType(BuilderParent parent) {
    return null;
  }

  @Override
  public com.google.protobuf.Message.Builder toBuilder() {
    return null;
  }

  @Override
  public com.google.protobuf.Message getDefaultInstanceForType() {
    return null;
  }

  @Override
  protected FieldAccessorTable internalGetFieldAccessorTable() {
    return null;
  }
}
