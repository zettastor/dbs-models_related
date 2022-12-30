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

package py.icshare;

import java.util.List;
import py.common.struct.EndPoint;
import py.informationcenter.Utils;

public class Operation {
  private Long operationId;
  private Long targetId;
  private TargetType targetType;
  private Long startTime;
  private Long endTime;
  private String description;
  private OperationStatus status;
  private Long progress;
  private String errorMessage;
  private Long accountId;
  private String accountName;
  private OperationType operationType;
  private String operationObject;
  private String targetName;
  // default this parameter is 0L, it delegate the size before extend, if the
  // operation type is extend
  private Long targetSourceSize;
  private List<EndPoint> endPointList;
  private Integer snapshotId;

  public Operation() {
    this.status = OperationStatus.ACTIVITING;
  }

  public Operation(Long operationId, Long targetId, TargetType targetType, Long startTime,
      Long progress,
      String errorMessage, Long accountId, OperationType operationType, String targetName) {
    this.operationId = operationId;
    this.targetId = targetId;
    this.targetType = targetType;
    this.startTime = startTime;
    this.progress = progress;
    this.errorMessage = errorMessage;
    this.accountId = accountId;
    this.operationType = operationType;
    this.targetName = targetName;
  }

  public OperationInformation toOperationInformation() {
    OperationInformation operationInformation = new OperationInformation();
    if (operationId != null) {
      operationInformation.setOperationId(operationId);
    }
    if (targetId != null) {
      operationInformation.setTargetId(targetId);
    }
    if (targetType != null) {
      operationInformation.setTargetType(String.valueOf(targetType));
    }
    if (startTime != null) {
      operationInformation.setStartTime(startTime);
    }
    if (endTime != null) {
      operationInformation.setEndTime(endTime);
    }
    if (description != null) {
      operationInformation.setDescription(description);
    }
    if (status != null) {
      operationInformation.setStatus(String.valueOf(status));
    }
    if (progress != null) {
      operationInformation.setProgress(progress);
    }
    if (errorMessage != null) {
      operationInformation.setErrorMessage(errorMessage);
    }
    if (accountId != null) {
      operationInformation.setAccountId(accountId);
    }
    if (operationType != null) {
      operationInformation.setOperationType(String.valueOf(operationType));
    }

    operationInformation.setOperationObject(operationObject);

    if (targetName != null) {
      operationInformation.setTargetName(targetName);
    }
    if (targetSourceSize != null) {
      operationInformation.setTargetSourceSize(targetSourceSize);
    }
    /*
     * String endPointListString = Utils.bulidJsonStrFromObjectEps(endPointList);
     * if (endPointListString != null) {
     * operationInformation.setEndPointList(endPointListString); }
     */
    String endPointListString = Utils.buildStrFromObjectEps(endPointList);
    operationInformation.setEndPointList(endPointListString);
    if (snapshotId != null) {
      operationInformation.setSnapshotId(snapshotId);
    }

    operationInformation.setAccountName(accountName);
    return operationInformation;
  }

  public Long getOperationId() {
    return operationId;
  }

  public void setOperationId(Long operationId) {
    this.operationId = operationId;
  }

  public Long getTargetId() {
    return targetId;
  }

  public void setTargetId(Long targetId) {
    this.targetId = targetId;
  }

  public TargetType getTargetType() {
    return targetType;
  }

  public void setTargetType(TargetType targetType) {
    this.targetType = targetType;
  }

  public Long getStartTime() {
    return startTime;
  }

  public void setStartTime(Long startTime) {
    this.startTime = startTime;
  }

  public Long getEndTime() {
    return endTime;
  }

  public void setEndTime(Long endTime) {
    this.endTime = endTime;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public OperationStatus getStatus() {
    return status;
  }

  public void setStatus(OperationStatus status) {
    this.status = status;
  }

  public Long getProgress() {
    return progress;
  }

  public void setProgress(Long progress) {
    this.progress = progress;
  }

  public String getErrorMessage() {
    return errorMessage;
  }

  public void setErrorMessage(String errorMessage) {
    this.errorMessage = errorMessage;
  }

  public String getTargetName() {
    return targetName;
  }

  public void setTargetName(String targetName) {
    this.targetName = targetName;
  }

  public List<EndPoint> getEndPointList() {
    return endPointList;
  }

  public void setEndPointList(List<EndPoint> endPointList) {
    this.endPointList = endPointList;
  }

  @Override
  public String toString() {
    return "Operation [operationId=" + operationId + ", targetId=" + targetId + ", targetType="
        + targetType
        + ", startTime=" + startTime + ", endTime=" + endTime + ", description=" + description
        + ", status=" + status + ", progress=" + progress + ", errorMessage=" + errorMessage
        + ", accountId=" + accountId + ", accountName=" + accountName + ", operationType="
        + operationType
        + ", operationObject=" + operationObject + ", targetName=" + targetName
        + ", targetSourceSize=" + targetSourceSize + ", endPointList=" + endPointList + "]";
  }

  public Long getAccountId() {
    return accountId;
  }

  public void setAccountId(Long accountId) {
    this.accountId = accountId;
  }

  public OperationType getOperationType() {
    return operationType;
  }

  public void setOperationType(OperationType operationType) {
    this.operationType = operationType;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((accountId == null) ? 0 : accountId.hashCode());
    result = prime * result + ((accountName == null) ? 0 : accountName.hashCode());
    result = prime * result + ((description == null) ? 0 : description.hashCode());
    result = prime * result + ((endPointList == null) ? 0 : endPointList.hashCode());
    result = prime * result + ((endTime == null) ? 0 : endTime.hashCode());
    result = prime * result + ((errorMessage == null) ? 0 : errorMessage.hashCode());
    result = prime * result + ((operationId == null) ? 0 : operationId.hashCode());
    result = prime * result + ((targetType == null) ? 0 : targetType.hashCode());
    result = prime * result + ((operationType == null) ? 0 : operationType.hashCode());
    result = prime * result + ((progress == null) ? 0 : progress.hashCode());
    result = prime * result + ((snapshotId == null) ? 0 : snapshotId.hashCode());
    result = prime * result + ((startTime == null) ? 0 : startTime.hashCode());
    result = prime * result + ((status == null) ? 0 : status.hashCode());
    result = prime * result + ((targetId == null) ? 0 : targetId.hashCode());
    result = prime * result + ((operationObject == null) ? 0 : operationObject.hashCode());
    result = prime * result + ((targetName == null) ? 0 : targetName.hashCode());
    result = prime * result + ((targetSourceSize == null) ? 0 : targetSourceSize.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    Operation other = (Operation) obj;
    if (accountId == null) {
      if (other.accountId != null) {
        return false;
      }
    } else if (!accountId.equals(other.accountId)) {
      return false;
    }
    if (description == null) {
      if (other.description != null) {
        return false;
      }
    } else if (!description.equals(other.description)) {
      return false;
    }
    if (endPointList == null) {
      if (other.endPointList != null) {
        return false;
      }
    } else if (!endPointList.equals(other.endPointList)) {
      return false;
    }
    if (endTime == null) {
      if (other.endTime != null) {
        return false;
      }
    } else if (!endTime.equals(other.endTime)) {
      return false;
    }
    if (errorMessage == null) {
      if (other.errorMessage != null) {
        return false;
      }
    } else if (!errorMessage.equals(other.errorMessage)) {
      return false;
    }
    if (operationId == null) {
      if (other.operationId != null) {
        return false;
      }
    } else if (!operationId.equals(other.operationId)) {
      return false;
    }
    if (targetType != other.targetType) {
      return false;
    }
    if (operationType != other.operationType) {
      return false;
    }
    if (progress == null) {
      if (other.progress != null) {
        return false;
      }
    } else if (!progress.equals(other.progress)) {
      return false;
    }
    if (snapshotId == null) {
      if (other.snapshotId != null) {
        return false;
      }
    } else if (!snapshotId.equals(other.snapshotId)) {
      return false;
    }
    if (startTime == null) {
      if (other.startTime != null) {
        return false;
      }
    } else if (!startTime.equals(other.startTime)) {
      return false;
    }
    if (status != other.status) {
      return false;
    }
    if (targetId == null) {
      if (other.targetId != null) {
        return false;
      }
    } else if (!targetId.equals(other.targetId)) {
      return false;
    }
    if (operationObject == null) {
      if (other.operationObject != null) {
        return false;
      }
    } else if (!operationObject.equals(other.operationObject)) {
      return false;
    }
    if (targetName == null) {
      if (other.targetName != null) {
        return false;
      }
    } else if (!targetName.equals(other.targetName)) {
      return false;
    }
    if (targetSourceSize == null) {
      if (other.targetSourceSize != null) {
        return false;
      }
    } else if (!targetSourceSize.equals(other.targetSourceSize)) {
      return false;
    }
    if (accountName == null) {
      if (other.accountName != null) {
        return false;
      }
    } else if (!accountName.equals(other.accountName)) {
      return false;
    }
    return true;
  }

  public Long getTargetSourceSize() {
    return targetSourceSize;
  }

  public void setTargetSourceSize(Long targetSourceSize) {
    this.targetSourceSize = targetSourceSize;
  }

  public Integer getSnapshotId() {
    return snapshotId;
  }

  public void setSnapshotId(Integer snapshotId) {
    this.snapshotId = snapshotId;
  }

  public String getAccountName() {
    return accountName;
  }

  public void setAccountName(String accountName) {
    this.accountName = accountName;
  }

  public String getOperationObject() {
    return operationObject;
  }

  public void setOperationObject(String operationObject) {
    this.operationObject = operationObject;
  }
}
