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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.common.struct.EndPoint;
import py.informationcenter.Utils;

public class OperationInformation {
  private static final Logger logger = LoggerFactory.getLogger(OperationInformation.class);

  private Long operationId;
  private Long targetId;
  private String targetType;
  private Long startTime;
  private Long endTime;
  private String description;
  private String status;
  private Long progress;
  private String errorMessage;
  private Long accountId;
  private String accountName;
  private String operationType;
  private String operationObject;
  private String targetName;
  private Long targetSourceSize;
  private String endPointList;
  private Integer snapshotId;

  public Operation toOperation() {
    logger.debug("begin transition operationInformation to operation, operationInformation is : {}",
        this);

    Operation operation = new Operation();
    if (operationId != null) {
      operation.setOperationId(operationId);
    }
    if (targetId != null) {
      operation.setTargetId(targetId);
    }
    if (targetType != null) {
      operation.setTargetType(TargetType.valueOf(targetType));
    }
    if (startTime != null) {
      operation.setStartTime(startTime);
    }
    if (endTime != null) {
      operation.setEndTime(endTime);
    }
    if (description != null) {
      operation.setDescription(description);
    }
    if (status != null) {
      operation.setStatus(OperationStatus.valueOf(status));
    }
    if (progress != null) {
      operation.setProgress(progress);
    }
    if (errorMessage != null) {
      operation.setErrorMessage(errorMessage);
    }
    if (accountId != null) {
      operation.setAccountId(accountId);
    }
    if (operationType != null) {
      operation.setOperationType(OperationType.valueOf(operationType));
    }

    operation.setOperationObject(operationObject);

    if (targetName != null) {
      operation.setTargetName(targetName);
    }
    if (targetSourceSize != null) {
      operation.setTargetSourceSize(targetSourceSize);
    }
    List<EndPoint> endPoints = Utils.parseObjectFromStrEps(endPointList);
    operation.setEndPointList(endPoints);
    if (snapshotId != null) {
      operation.setSnapshotId(snapshotId);
    }

    operation.setAccountName(accountName);
    return operation;
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

  public String getTargetType() {
    return targetType;
  }

  public void setTargetType(String targetType) {
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

  public String getStatus() {
    return status;
  }

  public void setStatus(String status) {
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

  @Override
  public String toString() {
    return "OperationInformation [operationId=" + operationId + ", targetId=" + targetId
        + ", targetType=" + targetType + ", startTime=" + startTime + ", endTime=" + endTime
        + ", description=" + description + ", status=" + status + ", progress=" + progress
        + ", errorMessage=" + errorMessage + ", accountId=" + accountId + ", accountName="
        + accountName
        + ", operationType=" + operationType + ", operationObject=" + operationType
        + ", targetName=" + targetName + ", targetSourceSize=" + targetSourceSize
        + ", endPointList=" + endPointList + ", snapshotId=" + snapshotId + "]";
  }

  public Long getAccountId() {
    return accountId;
  }

  public void setAccountId(Long accountId) {
    this.accountId = accountId;
  }

  public String getOperationType() {
    return operationType;
  }

  public void setOperationType(String operationType) {
    this.operationType = operationType;
  }

  public String getTargetName() {
    return targetName;
  }

  public void setTargetName(String targetName) {
    this.targetName = targetName;
  }

  public Long getTargetSourceSize() {
    return targetSourceSize;
  }

  public void setTargetSourceSize(Long targetSourceSize) {
    this.targetSourceSize = targetSourceSize;
  }

  public String getEndPointList() {
    return endPointList;
  }

  public void setEndPointList(String endPointList) {
    this.endPointList = endPointList;
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
