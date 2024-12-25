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
