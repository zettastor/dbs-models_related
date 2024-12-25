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

package py.icshare;

import java.sql.Blob;
import javax.persistence.Lob;
import org.hibernate.annotations.Type;

public class ScsiClientDb {
  private String ipName;

  @Lob
  @Type(type = "org.hibernate.type.BlobType")
  private Blob scsiClientInfos;

  public ScsiClientDb() {
  }

  public String getIpName() {
    return ipName;
  }

  public void setIpName(String ipName) {
    this.ipName = ipName;
  }

  public Blob getScsiClientInfos() {
    return scsiClientInfos;
  }

  public void setScsiClientInfos(Blob scsiClientInfos) {
    this.scsiClientInfos = scsiClientInfos;
  }

  @Override
  public String toString() {
    return "ScsiClientDB{"
        + "ipName='" + ipName + '\''
        + ", scsiClientInfos=" + scsiClientInfos
        + '}';
  }
}
