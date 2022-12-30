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

package py.archive;

import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UnsettledArchiveMetadata extends ArchiveMetadata {
  private static final Logger logger = LoggerFactory.getLogger(UnsettledArchiveMetadata.class);

  private List<ArchiveType> originalType = new ArrayList<>();

  public UnsettledArchiveMetadata() {
  }

  public UnsettledArchiveMetadata(ArchiveMetadata archiveMetadata) {
    super(archiveMetadata);
  }

  public List<ArchiveType> getOriginalType() {
    return originalType;
  }

  public void setOriginalType(List<ArchiveType> originalType) {
    this.originalType = originalType;
  }

  public void addOriginalType(ArchiveType archiveType) {
    originalType.add(archiveType);
  }

  @Override
  public String toString() {
    return "UnsettledArchiveMetadata{}" + super.toString();
  }
}
