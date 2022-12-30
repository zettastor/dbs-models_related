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
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.netty.message.Header;
import py.netty.message.Message;
import py.netty.message.MessageCarryDataInterface;
import py.proto.Broadcastlog.PbIoUnitResult;
import py.proto.Broadcastlog.PbReadResponse;
import py.proto.Broadcastlog.PbReadResponseUnit;

public class PyReadResponse extends GeneratedMessage implements MessageCarryDataInterface {
  private static final Logger logger = LoggerFactory.getLogger(PyReadResponse.class);
  private static Method parseFrom;

  static {
    try {
      parseFrom = PbReadResponse.class.getMethod("parseFrom", CodedInputStream.class);
    } catch (NoSuchMethodException | SecurityException e) {
      throw new RuntimeException("can init class PYWriteRequest");
    }
  }

  private final PbReadResponse metadata;
  private ByteBuf data;
  private int[] offsetsInDataRange;

  public PyReadResponse(PbReadResponse metadata, ByteBuf data, boolean needInitialized) {
    this.metadata = metadata;
    this.data = data;
    if (needInitialized) {
      initDataOffsets();
    }
  }

  public PyReadResponse(PbReadResponse metadata, ByteBuf data) {
    this(metadata, data, true);
  }

  public static PyReadResponse parseFrom(Message msg, CodedInputStream inputStream)
      throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {
    Header header = msg.getHeader();
    ByteBuf body = msg.getBuffer();

    PbReadResponse metadata = (PbReadResponse) parseFrom.invoke(null, inputStream);
    Validate.isTrue(header.getDataLength() == body.readableBytes());
    if (header.getDataLength() > 0) {
      /*
       * pass msg to user, and user make sure call PYReadResponse.release()
       */
      return new PyReadResponse(metadata, body);
    } else {
      Validate.isTrue(header.getDataLength() == 0);
      /*
       * if we do NOT pass msg to user, should release msg here, because no one will release msg
       * if we don't.
       */
      msg.release();
      return new PyReadResponse(metadata, null);
    }
  }

  public static Class<?> getMetadataType() {
    return PbReadResponse.class;
  }

  /**
   * initial the map of the response-unit index to the data offset in the whole ByteBuf.
   */
  public PyReadResponse initDataOffsets() {
    if (this.offsetsInDataRange != null) {
      return this;
    }

    this.offsetsInDataRange = new int[metadata.getResponseUnitsCount()];
    int numberOfResponseUnits = metadata.getResponseUnitsCount();
    int responseUnitDataOffsetInWholeData = 0;
    for (int i = 0; i < numberOfResponseUnits; ++i) {
      PbReadResponseUnit responseUnit = metadata.getResponseUnits(i);
      if (responseUnit.getResult() == PbIoUnitResult.OK) {
        offsetsInDataRange[i] = responseUnitDataOffsetInWholeData;
        responseUnitDataOffsetInWholeData += responseUnit.getLength();
        continue;
      }

      logger.info("There is no data in {} {} {}", responseUnit.getOffset(),
          responseUnit.getLength(), responseUnit.getResult());
    }

    return this;
  }

  public ByteBuf getResponseUnitData(int index) {
    int length = metadata.getResponseUnits(index).getLength();
    if (length == 0) {
      return Unpooled.EMPTY_BUFFER;
    }
    return data.slice(data.readerIndex() + offsetsInDataRange[index], length).retain();
  }

  public ByteBuf getResponseUnitDataWithoutRetain(int index) {
    int length = metadata.getResponseUnits(index).getLength();
    if (length == 0) {
      return Unpooled.EMPTY_BUFFER;
    }
    return data.slice(data.readerIndex() + offsetsInDataRange[index], length);
  }

  public int getResponseUnitsCount() {
    return metadata.getResponseUnitsCount();
  }

  public PbReadResponse getMetadata() {
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

  public void release() {
    if (data != null) {
      data.release();
      data = null;
    }
  }

  @Override
  public void writeTo(final CodedOutputStream output) throws IOException {
    metadata.writeTo(output);
  }

  @Override
  public int getSerializedSize() {
    if (metadata == null) {
      return 0;
    }
    return metadata.getSerializedSize();
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
