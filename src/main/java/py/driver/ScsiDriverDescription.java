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

package py.driver;

public enum ScsiDriverDescription {
  Normal(1) {
    @Override
    public String getChinaDescription() {
      return "正常";
    }

    @Override
    public String getEnglishDescription() {
      return "Normal";
    }
  },

  ServiceHavingBeenShutdown(2) {
    @Override
    public String getChinaDescription() {
      return "服务已被关闭";
    }

    @Override
    public String getEnglishDescription() {
      return "Service has been shutdown";
    }
  },

  CreateVolumeAccessRulesError(3) {
    @Override
    public String getChinaDescription() {
      return "卷授权失败";
    }

    @Override
    public String getEnglishDescription() {
      return "Service has been shutdown";
    }
  },

  Error(4) {
    @Override
    public String getChinaDescription() {
      return "未知错误";
    }

    @Override
    public String getEnglishDescription() {
      return "unknow error";
    }
  },

  NoEnoughPydDeviceException(5) {
    @Override
    public String getChinaDescription() {
      return "缺少足够的PYD设备";
    }

    @Override
    public String getEnglishDescription() {
      return "Not enough Pyd device";
    }
  },

  ConnectPydDeviceOperationException(6) {
    @Override
    public String getChinaDescription() {
      return "连接块设备失败";
    }

    @Override
    public String getEnglishDescription() {
      return "Connect Pyd Device Exception";
    }
  },

  CreateBackstoresOperationException(7) {
    @Override
    public String getChinaDescription() {
      return "创建后端失败";
    }

    @Override
    public String getEnglishDescription() {
      return "Create Back stores Exception";
    }
  },

  CreateLoopbackOperationException(8) {
    @Override
    public String getChinaDescription() {
      return " 创建SCSI目标失败";
    }

    @Override
    public String getEnglishDescription() {
      return "Create Loopback Operation Exception";
    }
  },

  CreateLoopbackLunsOperationException(9) {
    @Override
    public String getChinaDescription() {
      return "创建Lun失败";
    }

    @Override
    public String getEnglishDescription() {
      return "Create Loopback Luns Operation Exception";
    }

  },

  GetScsiDeviceOperationException(10) {
    @Override
    public String getChinaDescription() {
      return "获取SCSI设备名称失败";
    }

    @Override
    public String getEnglishDescription() {
      return "Get Scsi Device Operation Exception";
    }
  },

  ScsiDeviceIsLaunchException(11) {
    @Override
    public String getChinaDescription() {
      return "卷在挂载中或者挂载过";
    }

    @Override
    public String getEnglishDescription() {
      return "Scsi　Device is Launch";
    }
  },

  VolumeNotFoundException(12) {
    @Override
    public String getChinaDescription() {
      return "挂载的卷不存在";
    }

    @Override
    public String getEnglishDescription() {
      return "can not find the volume";
    }
  },

  VolumeNotAvailableException(13) {
    @Override
    public String getChinaDescription() {
      return "卷不可用";
    }

    @Override
    public String getEnglishDescription() {
      return "Volume　Not　Available";
    }
  },

  VolumeWasRollbackingException(14) {
    @Override
    public String getChinaDescription() {
      return "当前卷存在快照回滚";
    }

    @Override
    public String getEnglishDescription() {
      return "Volume　Was　Rollbacking";
    }
  },

  VolumeLaunchMultiDriversException(15) {
    @Override
    public String getChinaDescription() {
      return "卷不可以挂载多次";
    }

    @Override
    public String getEnglishDescription() {
      return "Volume Launch MultiDrivers";
    }
  },

  DriverNameExistsException(16) {
    @Override
    public String getChinaDescription() {
      return "存在相同的驱动名";
    }

    @Override
    public String getEnglishDescription() {
      return "Driver Name Exists Exception";
    }
  },

  VolumeBeingDeletedException(17) {
    @Override
    public String getChinaDescription() {
      return "卷在删除中";
    }

    @Override
    public String getEnglishDescription() {
      return "Volume　Being　Deleted Exception";
    }
  },

  TooManyDriversException(18) {
    @Override
    public String getChinaDescription() {
      return "没有足够的挂载服务";
    }

    @Override
    public String getEnglishDescription() {
      return "small Drivers Exception";
    }
  },

  InfocenterServerException(19) {
    @Override
    public String getChinaDescription() {
      return "infocenter 服务异常或者网络异常";
    }

    @Override
    public String getEnglishDescription() {
      return "Infocenter Server Exception";
    }
  },

  CreateVolumeAccessRulesException(20) {
    @Override
    public String getChinaDescription() {
      return "创建卷规程异常";
    }

    @Override
    public String getEnglishDescription() {
      return "create Volume Access Rules Exception";
    }
  },

  ApplyVolumeAccessRuleException(21) {
    @Override
    public String getChinaDescription() {
      return "关联卷规则异常";
    }

    @Override
    public String getEnglishDescription() {
      return "Apply VolumeAccess Rule Exception";
    }
  },

  SystemMemoryIsNotEnough(22) {
    @Override
    public String getChinaDescription() {
      return "系统内存不足";
    }

    @Override
    public String getEnglishDescription() {
      return "System memory is not enough";
    }
  },

  DriverIsUpgradingException(23) {
    @Override
    public String getChinaDescription() {
      return "驱动正在升级中";
    }

    @Override
    public String getEnglishDescription() {
      return "Driver is upgrading";
    }
  },

  DriverTypeIsConflictException(24) {
    @Override
    public String getChinaDescription() {
      return "驱动类型冲突";
    }

    @Override
    public String getEnglishDescription() {
      return "Driver type is conflict";
    }
  },

  GetPydDriverStatusException(25) {
    @Override
    public String getChinaDescription() {
      return "获取驱动状态异常";
    }

    @Override
    public String getEnglishDescription() {
      return "Get Pyd Driver Status Exception";
    }
  },

  GetScsiClientException(26) {
    @Override
    public String getChinaDescription() {
      return "获取SCSI客户端异常";
    }

    @Override
    public String getEnglishDescription() {
      return "Get Scsi Client Exception";
    }
  },

  CanNotGetPydDriverException(27) {
    @Override
    public String getChinaDescription() {
      return "获取不到卸载的PYD驱动";
    }

    @Override
    public String getEnglishDescription() {
      return "Can Not Get Pyd Driver Exception";
    }
  },

  FailedToUmountDriverException(28) {
    @Override
    public String getChinaDescription() {
      return "卸载驱动失败";
    }

    @Override
    public String getEnglishDescription() {
      return "Umount driver failed";
    }
  },

  ExistsClientException(29) {
    @Override
    public String getChinaDescription() {
      return "存在正在连接的客户端";
    }

    @Override
    public String getEnglishDescription() {
      return "There are connected clients";
    }
  },

  DriverIsLaunchingException(30) {
    @Override
    public String getChinaDescription() {
      return "正在挂载驱动";
    }

    @Override
    public String getEnglishDescription() {
      return "Driver is launching, please try later";
    }
  },

  NetworkErrorException(31) {
    @Override
    public String getChinaDescription() {
      return "网络异常，可能导致操作失败";
    }

    @Override
    public String getEnglishDescription() {
      return "Network error, maybe the oeration failed";
    }
  },

  DriverContainerIsINCException(32) {
    @Override
    public String getChinaDescription() {
      return "驱动容器不可用";
    }

    @Override
    public String getEnglishDescription() {
      return "Driver container is INC";
    }
  },

  ServiceIsNotAvailable(33) {
    @Override
    public String getChinaDescription() {
      return "服务不可用";
    }

    @Override
    public String getEnglishDescription() {
      return "Service is not available";
    }
  },

  AccountNotFoundException(34) {
    @Override
    public String getChinaDescription() {
      return "用户权限不对";
    }

    @Override
    public String getEnglishDescription() {
      return "Account Not Found Exception";
    }
  },

  DriverUnmountingException(35) {
    @Override
    public String getChinaDescription() {
      return "驱动正在被卸载";
    }

    @Override
    public String getEnglishDescription() {
      return "Driver is being unmounted";
    }
  },

  DriverLaunchingException(36) {
    @Override
    public String getChinaDescription() {
      return "驱动正在被挂载";
    }

    @Override
    public String getEnglishDescription() {
      return "Driver is being launched";
    }
  },

  BeginLaunching(37) {
    @Override
    public String getChinaDescription() {
      return "驱动正在挂载中";
    }

    @Override
    public String getEnglishDescription() {
      return "Driver is in launching";
    }
  },

  BeginUnmounting(38) {
    @Override
    public String getChinaDescription() {
      return "驱动正在卸载中";
    }

    @Override
    public String getEnglishDescription() {
      return "Driver is in unmounting";
    }
  },

  ScsiVolumeLockException(39) {
    @Override
    public String getChinaDescription() {
      return "卷正在进行其他操作,请等待";
    }

    @Override
    public String getEnglishDescription() {
      return "volume is in other operation, please wait";
    }
  };

  private final int value;

  ScsiDriverDescription(int value) {
    this.value = value;
  }

  public static ScsiDriverDescription findByValue(int value) {
    switch (value) {
      case 1:
        return Normal;
      case 2:
        return ServiceHavingBeenShutdown;
      case 3:
        return CreateVolumeAccessRulesError;
      case 4:
        return Error;
      case 5:
        return NoEnoughPydDeviceException;
      case 6:
        return ConnectPydDeviceOperationException;
      case 7:
        return CreateBackstoresOperationException;
      case 8:
        return CreateLoopbackOperationException;
      case 9:
        return CreateLoopbackLunsOperationException;
      case 10:
        return GetScsiDeviceOperationException;
      case 11:
        return ScsiDeviceIsLaunchException;
      case 12:
        return VolumeNotFoundException;
      case 13:
        return VolumeNotAvailableException;
      case 14:
        return VolumeWasRollbackingException;
      case 15:
        return VolumeLaunchMultiDriversException;
      case 16:
        return DriverNameExistsException;
      case 17:
        return VolumeBeingDeletedException;
      case 18:
        return TooManyDriversException;
      case 19:
        return InfocenterServerException;
      case 20:
        return CreateVolumeAccessRulesException;
      case 21:
        return ApplyVolumeAccessRuleException;
      case 22:
        return SystemMemoryIsNotEnough;
      case 23:
        return DriverIsUpgradingException;
      case 24:
        return DriverTypeIsConflictException;
      case 25:
        return GetPydDriverStatusException;
      case 26:
        return GetScsiClientException;
      case 27:
        return CanNotGetPydDriverException;
      case 28:
        return FailedToUmountDriverException;
      case 29:
        return ExistsClientException;
      case 30:
        return DriverIsLaunchingException;
      case 31:
        return NetworkErrorException;
      case 32:
        return DriverContainerIsINCException;
      case 33:
        return ServiceIsNotAvailable;
      case 34:
        return AccountNotFoundException;
      case 35:
        return DriverUnmountingException;
      case 36:
        return DriverLaunchingException;
      case 37:
        return BeginLaunching;
      case 38:
        return BeginUnmounting;
      case 39:
        return ScsiVolumeLockException;

      default:
        return null;
    }
  }

  public static ScsiDriverDescription findByValue(String value) {
    if (value == null) {
      return null;
    }

    if (value.equals("Normal")) {
      return Normal;
    } else if (value.equals("ServiceHavingBeenShutdown")) {
      return ServiceHavingBeenShutdown;
    } else if (value.equals("CreateVolumeAccessRulesError")) {
      return CreateVolumeAccessRulesError;
    } else if (value.equals("Error")) {
      return Error;
    } else if (value.equals("NoEnoughPydDeviceException")) {
      return NoEnoughPydDeviceException;
    } else if (value.equals("ConnectPydDeviceOperationException")) {
      return ConnectPydDeviceOperationException;
    } else if (value.equals("CreateBackstoresOperationException")) {
      return CreateBackstoresOperationException;
    } else if (value.equals("CreateLoopbackOperationException")) {
      return CreateLoopbackOperationException;
    } else if (value.equals("CreateLoopbackLunsOperationException")) {
      return CreateLoopbackLunsOperationException;
    } else if (value.equals("GetScsiDeviceOperationException")) {
      return GetScsiDeviceOperationException;
    } else if (value.equals("ScsiDeviceIsLaunchException")) {
      return ScsiDeviceIsLaunchException;
    } else if (value.equals("VolumeNotFoundException")) {
      return VolumeNotFoundException;
    } else if (value.equals("VolumeNotAvailableException")) {
      return VolumeNotAvailableException;
    } else if (value.equals("VolumeWasRollbackingException")) {
      return VolumeWasRollbackingException;
    } else if (value.equals("VolumeLaunchMultiDriversException")) {
      return VolumeLaunchMultiDriversException;
    } else if (value.equals("DriverNameExistsException")) {
      return DriverNameExistsException;
    } else if (value.equals("VolumeBeingDeletedException")) {
      return VolumeBeingDeletedException;
    } else if (value.equals("TooManyDriversException")) {
      return TooManyDriversException;
    } else if (value.equals("InfocenterServerException")) {
      return InfocenterServerException;
    } else if (value.equals("CreateVolumeAccessRulesException")) {
      return CreateVolumeAccessRulesException;
    } else if (value.equals("ApplyVolumeAccessRuleException")) {
      return ApplyVolumeAccessRuleException;
    } else if (value.equals("SystemMemoryIsNotEnough")) {
      return SystemMemoryIsNotEnough;
    } else if (value.equals("DriverIsUpgradingException")) {
      return DriverIsUpgradingException;
    } else if (value.equals("DriverTypeIsConflictException")) {
      return DriverTypeIsConflictException;
    } else if (value.equals("GetPydDriverStatusException")) {
      return GetPydDriverStatusException;
    } else if (value.equals("GetScsiClientException")) {
      return GetScsiClientException;
    } else if (value.equals("CanNotGetPydDriverException")) {
      return CanNotGetPydDriverException;
    } else if (value.equals("FailedToUmountDriverException")) {
      return FailedToUmountDriverException;
    } else if (value.equals("ExistsClientException")) {
      return ExistsClientException;
    } else if (value.equals("DriverIsLaunchingException")) {
      return DriverIsLaunchingException;
    } else if (value.equals("NetworkErrorException")) {
      return NetworkErrorException;
    } else if (value.equals("DriverContainerIsINCException")) {
      return DriverContainerIsINCException;
    } else if (value.equals("ServiceIsNotAvailable")) {
      return ServiceIsNotAvailable;
    } else if (value.equals("AccountNotFoundException")) {
      return AccountNotFoundException;
    } else if (value.equals("DriverUnmountingException")) {
      return DriverUnmountingException;
    } else if (value.equals("DriverLaunchingException")) {
      return DriverLaunchingException;
    } else if (value.equals("BeginLaunching")) {
      return BeginLaunching;
    } else if (value.equals("BeginUnmounting")) {
      return BeginUnmounting;
    } else if (value.equals("ScsiVolumeLockException")) {
      return ScsiVolumeLockException;
    } else {
      return null;
    }
  }

  public int getValue() {
    return value;
  }

  public String getChinaDescription() {
    return null;
  }

  public String getEnglishDescription() {
    return null;
  }

}
