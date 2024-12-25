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

package py.icshare.exception;

public class VolumeNotFoundException extends Exception {
  private static final long serialVersionUID = 4458448349366090189L;

  private long volumeId;

  public VolumeNotFoundException() {
    super();
  }

  public VolumeNotFoundException(long volumeId) {
    super();
    this.volumeId = volumeId;
  }

  public VolumeNotFoundException(String message) {
    super(message);
  }

  public VolumeNotFoundException(String message, Throwable cause) {
    super(message, cause);
  }

  public VolumeNotFoundException(Throwable cause) {
    super(cause);
  }

  public long getVolumeId() {
    return volumeId;
  }

  public void setVolumeId(long volumeId) {
    this.volumeId = volumeId;
  }
}
