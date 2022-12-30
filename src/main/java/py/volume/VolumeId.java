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

package py.volume;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.nio.ByteBuffer;
import py.common.struct.AbstractId;
import py.exception.InvalidFormatException;

/**
 * Placehold for volume id.
 *
 */
public class VolumeId extends AbstractId {
  public static final int BYTES = Long.BYTES;

  @JsonCreator
  public VolumeId(@JsonProperty("id") long id) {
    super(id);
  }

  public VolumeId(String str) throws InvalidFormatException {
    super(str);
  }

  public VolumeId(byte[] bytes) throws InvalidFormatException {
    super(bytes);
  }

  public VolumeId(ByteBuffer buffer) throws InvalidFormatException {
    super(buffer);
  }

  @Override
  public String printablePrefix() {
    return "vid";
  }
}
