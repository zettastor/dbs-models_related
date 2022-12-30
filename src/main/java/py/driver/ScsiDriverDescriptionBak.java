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

public enum ScsiDriverDescriptionBak {
  Normal(1, "正常", "正常状态", "不需要") {
    @Override
    public String getChinaDescription() {
      return "正常";
    }

    @Override
    public String getEnglishDescription() {
      return "Normal";
    }
  },

  ServiceHavingBeenShutdown(2, "服务已被关闭", "服务不可用", "查看服务状态,启动对应服务") {
    @Override
    public String getChinaDescription() {
      return "服务已被关闭";
    }

    @Override
    public String getEnglishDescription() {
      return "Service has been shutdown";
    }
  },

  CreateVolumeAccessRulesError(3, "卷授权失败", "内部异常错误", "联系管理员") {
    @Override
    public String getChinaDescription() {
      return "卷授权失败";
    }

    @Override
    public String getEnglishDescription() {
      return "Service has been shutdown";
    }
  },

  Error(4, "未知错误", "系统未知异常", "查看网络和服务状态") {
    @Override
    public String getChinaDescription() {
      return "未知错误";
    }

    @Override
    public String getEnglishDescription() {
      return "unknow error";
    }
  },

  NoEnoughPydDeviceException(5, "缺少足够的PYD设备", "PYD设备到达上限", "建议挂载卷到其他的客户端") {
    @Override
    public String getChinaDescription() {
      return "缺少足够的PYD设备";
    }

    @Override
    public String getEnglishDescription() {
      return "Not enough Pyd device";
    }
  },

  ConnectPydDeviceOperationException(6, "连接块设备失败", "驱动容器内部错误", "建议查询客户端对应机器") {
    @Override
    public String getChinaDescription() {
      return "连接块设备失败";
    }

    @Override
    public String getEnglishDescription() {
      return "Connect Pyd Device Exception";
    }
  },

  CreateBackstoresOperationException(7, "创建后端失败", "驱动容器内部错误", "联系管理员") {
    @Override
    public String getChinaDescription() {
      return "创建后端失败";
    }

    @Override
    public String getEnglishDescription() {
      return "Create Back stores Exception";
    }
  },

  CreateLoopbackOperationException(8, "创建SCSI目标失败", "驱动容器内部错误", "查看对应客户端机器信息") {
    @Override
    public String getChinaDescription() {
      return " 创建SCSI目标失败";
    }

    @Override
    public String getEnglishDescription() {
      return "Create Loopback Operation Exception";
    }
  },

  CreateLoopbackLunsOperationException(9, "创建Lun失败", "驱动容器内部错误", "查看对应客户端机器信息") {
    @Override
    public String getChinaDescription() {
      return "创建Lun失败";
    }

    @Override
    public String getEnglishDescription() {
      return "Create Loopback Luns Operation Exception";
    }

  },

  GetScsiDeviceOperationException(10, "获取SCSI设备名称失败", "驱动容器内部错误", "查看对应客户端机器信息") {
    @Override
    public String getChinaDescription() {
      return "获取SCSI设备名称失败";
    }

    @Override
    public String getEnglishDescription() {
      return "Get Scsi Device Operation Exception";
    }
  },

  ScsiDeviceIsLaunchException(11, "卷在挂载中或者挂载过", "当前卷可能在挂载中或者已经挂载", "查看对应卷是否在其他客户端") {
    @Override
    public String getChinaDescription() {
      return "卷在挂载中或者挂载过";
    }

    @Override
    public String getEnglishDescription() {
      return "Scsi　Device is Launch";
    }
  },

  VolumeNotFoundException(12, "挂载的卷不存在", "可能误操作导致", "查看对应的卷信息,确认卷是否存在") {
    @Override
    public String getChinaDescription() {
      return "挂载的卷不存在";
    }

    @Override
    public String getEnglishDescription() {
      return "can not find the volume";
    }
  },

  VolumeNotAvailableException(13, "卷不可用", "可能DataNode服务异常导致", "查询DataNode服务状态") {
    @Override
    public String getChinaDescription() {
      return "卷不可用";
    }

    @Override
    public String getEnglishDescription() {
      return "Volume　Not　Available";
    }
  },

  VolumeWasRollbackingException(14, "当前卷存在快照回滚", "卷上的快照在回滚", "等待回滚结束再挂载") {
    @Override
    public String getChinaDescription() {
      return "当前卷存在快照回滚";
    }

    @Override
    public String getEnglishDescription() {
      return "Volume　Was　Rollbacking";
    }
  },

  VolumeLaunchMultiDriversException(15, "卷不可以挂载多次", "当前卷不能重复挂载到不同客户端", "查询和设置卷信息") {
    @Override
    public String getChinaDescription() {
      return "卷不可以挂载多次";
    }

    @Override
    public String getEnglishDescription() {
      return "Volume Launch MultiDrivers";
    }
  },

  DriverNameExistsException(16, "存在相同的驱动名", "内部处理异常", "建议卸载后重新挂载") {
    @Override
    public String getChinaDescription() {
      return "存在相同的驱动名";
    }

    @Override
    public String getEnglishDescription() {
      return "Driver Name Exists Exception";
    }
  },

  VolumeBeingDeletedException(17, "卷在删除中", "当前卷被执行删除操作", "可以重新创建卷挂载") {
    @Override
    public String getChinaDescription() {
      return "卷在删除中";
    }

    @Override
    public String getEnglishDescription() {
      return "Volume　Being　Deleted Exception";
    }
  },

  TooManyDriversException(18, "没有足够的挂载服务", "没有足够的挂载容器", "建议部署更多的挂载容器") {
    @Override
    public String getChinaDescription() {
      return "没有足够的挂载服务";
    }

    @Override
    public String getEnglishDescription() {
      return "small Drivers Exception";
    }
  },

  InfocenterServerException(19, "infocenter 服务异常或者网络异常", "服务机器异常", "查询和确认对应的服务是否异常,重新启动") {
    @Override
    public String getChinaDescription() {
      return "infocenter 服务异常或者网络异常";
    }

    @Override
    public String getEnglishDescription() {
      return "Infocenter Server Exception";
    }
  },

  ApplyVolumeAccessRuleException(20, "关联卷规则异常", "内部异常", "联系管理员") {
    @Override
    public String getChinaDescription() {
      return "关联卷规则异常";
    }

    @Override
    public String getEnglishDescription() {
      return "Apply VolumeAccess Rule Exception";
    }
  },

  SystemMemoryIsNotEnough(21, "系统内存不足", "挂载容器内存不足", "增加对应容器内存") {
    @Override
    public String getChinaDescription() {
      return "系统内存不足";
    }

    @Override
    public String getEnglishDescription() {
      return "System memory is not enough";
    }
  },

  DriverIsUpgradingException(22, "驱动正在升级中", "驱动在升级", "建议等待完成再进行操作") {
    @Override
    public String getChinaDescription() {
      return "驱动正在升级中";
    }

    @Override
    public String getEnglishDescription() {
      return "Driver is upgrading";
    }
  },

  DriverTypeIsConflictException(23, "驱动类型冲突", "内部异常", "联系管理员") {
    @Override
    public String getChinaDescription() {
      return "驱动类型冲突";
    }

    @Override
    public String getEnglishDescription() {
      return "Driver type is conflict";
    }
  },

  GetPydDriverStatusException(24, "获取驱动状态异常", "内部错误", "联系管理员") {
    @Override
    public String getChinaDescription() {
      return "获取驱动状态异常";
    }

    @Override
    public String getEnglishDescription() {
      return "Get Pyd Driver Status Exception";
    }
  },

  GetScsiClientException(25, "获取SCSI客户端异常", "客户端状态异常", "查询客户端状态,启动对应服务") {
    @Override
    public String getChinaDescription() {
      return "获取SCSI客户端异常";
    }

    @Override
    public String getEnglishDescription() {
      return "Get Scsi Client Exception";
    }
  },

  CanNotGetPydDriverException(26, "获取不到卸载的PYD驱动", "找不到要卸载的卷容器", "联系管理员") {
    @Override
    public String getChinaDescription() {
      return "获取不到卸载的PYD驱动";
    }

    @Override
    public String getEnglishDescription() {
      return "Can Not Get Pyd Driver Exception";
    }
  },

  FailedToUmountDriverException(27, "卸载驱动失败", "内部错误", "建议查询容器服务状态") {
    @Override
    public String getChinaDescription() {
      return "卸载驱动失败";
    }

    @Override
    public String getEnglishDescription() {
      return "Umount driver failed";
    }
  },

  ExistsClientException(28, "存在正在连接的客户端", "存在连接", "查询当前卷的连接状态") {
    @Override
    public String getChinaDescription() {
      return "存在正在连接的客户端";
    }

    @Override
    public String getEnglishDescription() {
      return "There are connected clients";
    }
  },

  DriverIsLaunchingException(29, "正在挂载驱动", "卷在挂载中", "等待挂载完成再进行下一步操作") {
    @Override
    public String getChinaDescription() {
      return "正在挂载驱动";
    }

    @Override
    public String getEnglishDescription() {
      return "Driver is launching, please try later";
    }
  },

  NetworkErrorException(30, "网络异常，可能导致操作失败", "可能网络状态不好", "查询当前集群的网络状态") {
    @Override
    public String getChinaDescription() {
      return "网络异常，可能导致操作失败";
    }

    @Override
    public String getEnglishDescription() {
      return "Network error, maybe the oeration failed";
    }
  },

  DriverContainerIsINCException(31, "驱动容器不可用", "驱动容器服务异常", "检查驱动容器状态") {
    @Override
    public String getChinaDescription() {
      return "驱动容器不可用";
    }

    @Override
    public String getEnglishDescription() {
      return "Driver container is INC";
    }
  },

  ServiceIsNotAvailable(32, "服务不可用", "内部错误", "查询各个服务状态") {
    @Override
    public String getChinaDescription() {
      return "服务不可用";
    }

    @Override
    public String getEnglishDescription() {
      return "Service is not available";
    }
  },

  AccountNotFoundException(33, "用户权限不对", "用户操作权限", "确认当前用户是否可以进行对应的操作") {
    @Override
    public String getChinaDescription() {
      return "用户权限不对";
    }

    @Override
    public String getEnglishDescription() {
      return "Account Not Found Exception";
    }
  },

  DriverUnmountingException(34, "驱动正在被卸载", "当前卷正在被卸载", "等待卸载完成") {
    @Override
    public String getChinaDescription() {
      return "驱动正在被卸载";
    }

    @Override
    public String getEnglishDescription() {
      return "Driver is being unmounted";
    }
  },

  DriverLaunchingException(35, "驱动正在被挂载", "当前卷正在被挂载", "等待挂载完成") {
    @Override
    public String getChinaDescription() {
      return "驱动正在被挂载";
    }

    @Override
    public String getEnglishDescription() {
      return "Driver is being launched";
    }
  };

  private int value;
  private String descriptionInfo;
  private String reasonInfo;
  private String suggestInfo;

  ScsiDriverDescriptionBak(int value, String descriptionInfo, String reasonInfo,
      String suggestInfo) {
    this.value = value;
    this.descriptionInfo = descriptionInfo;
    this.reasonInfo = reasonInfo;
    this.suggestInfo = suggestInfo;
  }

  ScsiDriverDescriptionBak(int value) {
    this.value = value;
  }

  public static ScsiDriverDescriptionBak findByValue(int value) {
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
        return ApplyVolumeAccessRuleException;
      case 21:
        return SystemMemoryIsNotEnough;
      case 22:
        return DriverIsUpgradingException;
      case 23:
        return DriverTypeIsConflictException;
      case 24:
        return GetPydDriverStatusException;
      case 25:
        return GetScsiClientException;
      case 26:
        return CanNotGetPydDriverException;
      case 27:
        return FailedToUmountDriverException;
      case 28:
        return ExistsClientException;
      case 29:
        return DriverIsLaunchingException;
      case 30:
        return NetworkErrorException;
      case 31:
        return DriverContainerIsINCException;
      case 32:
        return ServiceIsNotAvailable;
      case 33:
        return AccountNotFoundException;
      case 34:
        return DriverUnmountingException;
      case 35:
        return DriverLaunchingException;

      default:
        return null;
    }
  }

  public static ScsiDriverDescriptionBak findByValue(String value) {
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
    } else {
      return null;
    }
  }

  public String getReasonInfo() {
    return reasonInfo;
  }

  public String getSuggestInfo() {
    return suggestInfo;
  }

  public String getDescriptionInfo() {
    return descriptionInfo;
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
