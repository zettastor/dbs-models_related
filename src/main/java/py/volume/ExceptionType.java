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

package py.volume;

public enum ExceptionType {
  VolumeIsCopingExceptionThrift(1),
  VolumeInExtendingExceptionThrift(5),
  volumeIsCloningExceptionThrift(6),
  VolumeDeletingExceptionThrift(7),
  VolumeCyclingExceptionThrift(8),
  VolumeNotAvailableExceptionThrift(9);

  private final int value;

  ExceptionType(int value) {
    this.value = value;
  }

  public static String findByValue(int value) {
    switch (value) {
      case 1:
        return "VolumeIsCopingExceptionThrift";
      case 5:
        return "VolumeInExtendingExceptionThrift";
      case 6:
        return "volumeIsCloningExceptionThrift";
      case 7:
        return "VolumeDeletingExceptionThrift";
      case 8:
        return "VolumeCyclingExceptionThrift";
      case 9:
        return "VolumeNotAvailableExceptionThrift";

      default:
        return null;
    }
  }
}
