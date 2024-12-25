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

public class FlowerNotFoundException extends Exception {
  private static final long serialVersionUID = 4458448349366090189L;

  private long flowerId;

  public FlowerNotFoundException() {
    super();
  }

  public FlowerNotFoundException(long flowerId) {
    super();
    this.flowerId = flowerId;
  }

  public FlowerNotFoundException(String message) {
    super(message);
  }

  public FlowerNotFoundException(String message, Throwable cause) {
    super(message, cause);
  }

  public FlowerNotFoundException(Throwable cause) {
    super(cause);
  }

  public long getFlowerId() {
    return flowerId;
  }

  public void setFlowerId(long flowerId) {
    this.flowerId = flowerId;
  }
}
