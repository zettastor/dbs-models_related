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
import io.netty.buffer.Unpooled;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import org.apache.commons.lang.Validate;
import py.netty.message.Header;
import py.netty.message.Message;
import py.netty.message.MessageCarryDataInterface;
import py.proto.Broadcastlog.PbWriteRequest;

public class PyWriteRequest extends GeneratedMessage implements MessageCarryDataInterface {
  private static Method parseFrom;

  static {
    try {
      parseFrom = PbWriteRequest.class.getMethod("parseFrom", CodedInputStream.class);
    } catch (NoSuchMethodException | SecurityException e) {
      throw new RuntimeException("can init class PYWriteRequest");
    }
  }

  private final PbWriteRequest metadata;
  private final ByteBuf data;
  private int[] offsetsInDataRange;

  public PyWriteRequest(PbWriteRequest metadata, ByteBuf data) {
    this(metadata, data, true);
  }

  public PyWriteRequest(PbWriteRequest metadata, ByteBuf data, boolean needInitialized) {
    this.metadata = metadata;
    this.data = data;
    if (needInitialized) {
      initDataOffsets();
    }
  }

  public static PyWriteRequest parseFrom(Message msg, CodedInputStream inputStream)
      throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {
    Header header = msg.getHeader();
    ByteBuf body = msg.getBuffer();

    PbWriteRequest object = (PbWriteRequest) parseFrom.invoke(null, inputStream);
    /*
     * It's different from PYReadResponse, msg will be released finally when call method callback,
     * please watch AsyncRequestHandler.generateCallback(Message msg, ChannelHandlerContext ctx),
     * so no need release here even NOT pass msg to user.
     */
    if (header.getDataLength() > 0) {
      Validate.isTrue(header.getDataLength() == body.readableBytes());
      return new PyWriteRequest(object, body);
    } else {
      return new PyWriteRequest(object, null);
    }
  }

  public PyWriteRequest clone() {
    return new PyWriteRequest(metadata, data.retain(), this.offsetsInDataRange != null);
  }

  /**
   * initial the map of the response-unit index to the data offset in the whole ByteBuf.
   */
  private PyWriteRequest initDataOffsets() {
    if (this.offsetsInDataRange != null) {
      return this;
    }

    this.offsetsInDataRange = new int[metadata.getRequestUnitsCount()];
    int numberOfResponseUnits = metadata.getRequestUnitsCount();
    int responseUnitDataOffsetInWholeData = 0;
    for (int i = 0; i < numberOfResponseUnits; ++i) {
      offsetsInDataRange[i] = responseUnitDataOffsetInWholeData;
      responseUnitDataOffsetInWholeData += metadata.getRequestUnits(i).getLength();
    }
    return this;
  }

  public ByteBuf getRequestUnitData(int index) {
    int length = metadata.getRequestUnits(index).getLength();
    if (length == 0) {
      return Unpooled.EMPTY_BUFFER;
    }
    return data.slice(data.readerIndex() + offsetsInDataRange[index], length);
  }

  public PbWriteRequest getMetadata() {
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
