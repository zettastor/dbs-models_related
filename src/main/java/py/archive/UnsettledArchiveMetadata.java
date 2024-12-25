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
