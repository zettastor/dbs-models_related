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

package py;

import static py.thrift.share.VolumeSourceThrift.CREATE;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import java.lang.reflect.Method;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang.math.RandomUtils;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.ArchiveMetadata;
import py.archive.ArchiveStatus;
import py.archive.ArchiveType;
import py.archive.RawArchiveMetadata;
import py.archive.StorageType;
import py.archive.segment.SegId;
import py.archive.segment.SegmentMetadata;
import py.archive.segment.SegmentUnitMetadata;
import py.archive.segment.SegmentUnitStatus;
import py.archive.segment.SegmentUnitType;
import py.archive.segment.SegmentVersion;
import py.common.Constants;
import py.common.RequestIdBuilder;
import py.common.struct.EndPoint;
import py.common.struct.Pair;
import py.driver.DriverContainerCandidate;
import py.driver.DriverMetadata;
import py.driver.DriverStatus;
import py.driver.DriverType;
import py.driver.PortalType;
import py.icshare.AccessRuleStatusBindingVolume;
import py.icshare.AccountMetadata;
import py.icshare.BroadcastLogStatus;
import py.icshare.CapacityRecord;
import py.icshare.DiskInfo;
import py.icshare.DiskSmartInfo;
import py.icshare.Domain;
import py.icshare.DriverClientInformation;
import py.icshare.DriverClientKeyInformation;
import py.icshare.DriverKey;
import py.icshare.DriverKeyForScsi;
import py.icshare.InstanceMetadata;
import py.icshare.Operation;
import py.icshare.ScsiDriverMetadata;
import py.icshare.SensorInfo;
import py.icshare.ServerNode;
import py.icshare.TotalAndUsedCapacity;
import py.icshare.Volume2AccessRuleRelationship;
import py.icshare.VolumeAccessRule;
import py.icshare.VolumeCreationRequest;
import py.icshare.VolumeCreationRequest.RequestType;
import py.icshare.VolumeDeleteDelayInformation;
import py.icshare.VolumeRecycleInformation;
import py.icshare.authorization.ApiToAuthorize;
import py.icshare.authorization.PyResource;
import py.icshare.authorization.Role;
import py.icshare.iscsiaccessrule.Iscsi2AccessRuleRelationship;
import py.icshare.iscsiaccessrule.IscsiAccessRule;
import py.icshare.qos.CheckSecondaryInactiveThresholdMode;
import py.icshare.qos.IoLimitationRelationship;
import py.icshare.qos.IoLimitationStatusBindingDrivers;
import py.icshare.qos.MigrationRule;
import py.icshare.qos.MigrationRuleRelationship;
import py.icshare.qos.MigrationRuleStatusBindingPools;
import py.icshare.qos.RebalanceRule;
import py.informationcenter.AccessPermissionType;
import py.informationcenter.AccessRuleStatus;
import py.informationcenter.Status;
import py.informationcenter.StoragePool;
import py.informationcenter.StoragePoolStrategy;
import py.instance.Group;
import py.instance.Instance;
import py.instance.InstanceDomain;
import py.instance.InstanceId;
import py.instance.InstanceStore;
import py.instance.PortType;
import py.io.qos.IoLimitation;
import py.io.qos.IoLimitationEntry;
import py.io.qos.IoLimitationStatus;
import py.io.qos.MigrationRuleStatus;
import py.io.qos.MigrationStrategy;
import py.io.qos.RebalanceAbsoluteTime;
import py.io.qos.RebalanceRelativeTime;
import py.membership.SegmentMembership;
import py.monitor.common.AlertMessage;
import py.monitor.common.AlertRule;
import py.monitor.common.AlertTemplate;
import py.monitor.common.EventLogCompressed;
import py.monitor.common.EventLogInfo;
import py.monitor.common.PerformanceMessageHistory;
import py.monitor.common.PerformanceSearchTemplate;
import py.monitor.common.PerformanceTask;
import py.rebalance.RebalanceTask;
import py.thrift.datanode.service.BroadcastRequest;
import py.thrift.datanode.service.BroadcastTypeThrift;
import py.thrift.datanode.service.CreateSegmentUnitRequest;
import py.thrift.datanode.service.DeleteSegmentRequest;
import py.thrift.datanode.service.DeleteSegmentUnitRequest;
import py.thrift.datanode.service.DepartFromMembershipRequest;
import py.thrift.datanode.service.ReadRequest;
import py.thrift.datanode.service.ReadRequestUnit;
import py.thrift.datanode.service.UpdateSegmentUnitVolumeMetadataJsonRequest;
import py.thrift.datanode.service.WriteRequest;
import py.thrift.datanode.service.WriteRequestUnit;
import py.thrift.icshare.ScsiDescriptionTypeThrift;
import py.thrift.icshare.ScsiDeviceInfoThrift;
import py.thrift.infocenter.service.DriverContainerCandidateThrift;
import py.thrift.monitorserver.service.AlertMessageDetailThrift;
import py.thrift.monitorserver.service.AlertMessageThrift;
import py.thrift.monitorserver.service.AlertRuleThrift;
import py.thrift.monitorserver.service.AlertTemplateThrift;
import py.thrift.monitorserver.service.EventLogCompressedThrift;
import py.thrift.monitorserver.service.EventLogInfoThrift;
import py.thrift.monitorserver.service.PerformanceSearchTemplateThrift;
import py.thrift.monitorserver.service.PerformanceTaskThrift;
import py.thrift.share.AbsoluteTimethrift;
import py.thrift.share.AccessPermissionTypeThrift;
import py.thrift.share.AccessRuleStatusBindingVolumeThrift;
import py.thrift.share.AccessRuleStatusThrift;
import py.thrift.share.AccountMetadataBackupThrift;
import py.thrift.share.AccountMetadataThrift;
import py.thrift.share.AccountTypeThrift;
import py.thrift.share.ApiToAuthorizeThrift;
import py.thrift.share.ArchiveMetadataThrift;
import py.thrift.share.ArchiveStatusThrift;
import py.thrift.share.ArchiveTypeThrift;
import py.thrift.share.BroadcastLogStatusThrift;
import py.thrift.share.CapacityRecordThrift;
import py.thrift.share.CheckSecondaryInactiveThresholdModeThrift;
import py.thrift.share.CommitLogsRequestThrift;
import py.thrift.share.DatanodeStatusThrift;
import py.thrift.share.DatanodeTypeNotSetExceptionThrift;
import py.thrift.share.DatanodeTypeThrift;
import py.thrift.share.DiskSmartInfoThrift;
import py.thrift.share.DomainThrift;
import py.thrift.share.DriverClientInfoThrift;
import py.thrift.share.DriverKeyThrift;
import py.thrift.share.DriverMetadataThrift;
import py.thrift.share.DriverStatusThrift;
import py.thrift.share.DriverTypeThrift;
import py.thrift.share.GroupThrift;
import py.thrift.share.HardDiskInfoThrift;
import py.thrift.share.InstanceDomainThrift;
import py.thrift.share.InstanceIdAndEndPointThrift;
import py.thrift.share.InstanceMetadataThrift;
import py.thrift.share.IoLimitationEntryThrift;
import py.thrift.share.IoLimitationRelationShipThrift;
import py.thrift.share.IoLimitationStatusBindingDriversThrift;
import py.thrift.share.IoLimitationStatusThrift;
import py.thrift.share.IoLimitationThrift;
import py.thrift.share.IscsiAccessRuleThrift;
import py.thrift.share.IscsiRuleRelationshipThrift;
import py.thrift.share.LimitTypeThrift;
import py.thrift.share.MigrationRuleRelationShipThrift;
import py.thrift.share.MigrationRuleStatusBindingPoolsThrift;
import py.thrift.share.MigrationRuleStatusThrift;
import py.thrift.share.MigrationRuleThrift;
import py.thrift.share.MigrationStrategyThrift;
import py.thrift.share.OperationThrift;
import py.thrift.share.PortalTypeThrift;
import py.thrift.share.ReadWriteTypeThrift;
import py.thrift.share.RebalanceRulethrift;
import py.thrift.share.RebalanceTaskThrift;
import py.thrift.share.RebalanceTaskTypeThrift;
import py.thrift.share.RelativeTimethrift;
import py.thrift.share.ResourceThrift;
import py.thrift.share.RoleThrift;
import py.thrift.share.ScsiDeviceStatusThrift;
import py.thrift.share.SegIdThrift;
import py.thrift.share.SegmentMembershipSwitchThrift;
import py.thrift.share.SegmentMembershipThrift;
import py.thrift.share.SegmentMetadataSwitchThrift;
import py.thrift.share.SegmentMetadataThrift;
import py.thrift.share.SegmentUnitMetadataSwitchThrift;
import py.thrift.share.SegmentUnitMetadataThrift;
import py.thrift.share.SegmentUnitStatusThrift;
import py.thrift.share.SegmentUnitTypeThrift;
import py.thrift.share.SensorInfoThrift;
import py.thrift.share.ServerNodeThrift;
import py.thrift.share.StatusThrift;
import py.thrift.share.StoragePoolStrategyThrift;
import py.thrift.share.StoragePoolThrift;
import py.thrift.share.StorageTypeThrift;
import py.thrift.share.TotalAndUsedCapacityThrift;
import py.thrift.share.VolumeAccessRuleThrift;
import py.thrift.share.VolumeDeleteDelayInformationThrift;
import py.thrift.share.VolumeInActionThrift;
import py.thrift.share.VolumeMetadataThrift;
import py.thrift.share.VolumeRecycleInformationThrift;
import py.thrift.share.VolumeRuleRelationshipThrift;
import py.thrift.share.VolumeSourceThrift;
import py.thrift.share.VolumeStatusThrift;
import py.thrift.share.VolumeTypeThrift;
import py.thrift.share.WeekDaythrift;
import py.volume.CacheType;
import py.volume.VolumeInAction;
import py.volume.VolumeMetadata;
import py.volume.VolumeStatus;
import py.volume.VolumeType;

/**
 * helper class that converts internal classes to and from thrift classes.
 */
public class RequestResponseHelper {
  public static final int SHORT_MAX_VALUE = 65536;
  private static final Logger logger = LoggerFactory.getLogger(RequestResponseHelper.class);

  public static ArchiveMetadataThrift buildThriftArchiveMetadataFrom(
      ArchiveMetadata archiveMetadata) {
    Validate.notNull(archiveMetadata);
    ArchiveMetadataThrift archiveMetadataThrift = new ArchiveMetadataThrift(
        archiveMetadata.getDeviceName(),
        archiveMetadata.getArchiveId(),
        ArchiveStatusThrift.valueOf(archiveMetadata.getStatus().name()),
        ArchiveTypeThrift.valueOf(archiveMetadata.getArchiveType().name()), false,
        StorageTypeThrift.valueOf(archiveMetadata.getStorageType().name()));

    archiveMetadataThrift.setSlotNo(archiveMetadata.getSlotNo());
    archiveMetadataThrift.setCreatedBy(archiveMetadata.getCreatedBy());
    archiveMetadataThrift.setUpdatedBy(archiveMetadata.getUpdatedBy());
    archiveMetadataThrift.setCreatedTime(archiveMetadata.getCreatedTime());
    archiveMetadataThrift.setUpdatedTime(archiveMetadata.getUpdatedTime());
    archiveMetadataThrift.setSerialNumber(archiveMetadata.getSerialNumber());
    archiveMetadataThrift.setLogicalSpace(archiveMetadata.getLogicalSpace());
    switch (archiveMetadata.getArchiveType()) {
      case RAW_DISK:
        RawArchiveMetadata rawArchiveMetadata = (RawArchiveMetadata) archiveMetadata;
        archiveMetadataThrift.setOverloadedIsSet(rawArchiveMetadata.isOverloaded());
        archiveMetadataThrift.setLogicalFreeSpace(rawArchiveMetadata.getLogicalFreeSpace());
        if (rawArchiveMetadata.getStoragePoolId() != null) {
          archiveMetadataThrift.setStoragePoolId(rawArchiveMetadata.getStoragePoolId());
        }
        archiveMetadataThrift.setTotalPageToMigrate(rawArchiveMetadata.getTotalPageToMigrate());
        archiveMetadataThrift.setAlreadyMigratedPage(rawArchiveMetadata.getAlreadyMigratedPage());
        archiveMetadataThrift.setMigrationSpeed(rawArchiveMetadata.getMigrationSpeed());
        archiveMetadataThrift.setMaxMigrationSpeed(rawArchiveMetadata.getMaxMigrationSpeed());
        archiveMetadataThrift
            .setFreeFlexibleSegmentUnitCount(rawArchiveMetadata.getFreeFlexibleSegmentUnitCount());
        archiveMetadataThrift.setWeight(rawArchiveMetadata.getWeight());
        archiveMetadataThrift.setDataSizeMb(rawArchiveMetadata.getDataSizeMb());
        archiveMetadataThrift.setLogicalUsedSpace(rawArchiveMetadata.getUsedSpace());
        break;
      case UNSETTLED_DISK:
        archiveMetadataThrift.setStoragePoolId(0L);
        archiveMetadataThrift.setLogicalFreeSpace(archiveMetadata.getLogicalSpace());
        break;
      default:
        throw new RuntimeException("not support the archive=" + archiveMetadata);
    }
    return archiveMetadataThrift;
  }

  public static RawArchiveMetadata buildArchiveMetadataFrom(
      ArchiveMetadataThrift archiveMetadataThrift) {
    Validate.notNull(archiveMetadataThrift);

    ArchiveMetadata archiveMetadataBase = buildOtherArchiveMetadataFrom(archiveMetadataThrift);

    RawArchiveMetadata archiveMetadata = new RawArchiveMetadata(archiveMetadataBase);
    archiveMetadata.setLogicalSpace(archiveMetadataThrift.getLogicalSpace());
    archiveMetadata.setLogicalFreeSpace(archiveMetadataThrift.getLogicalFreeSpace());
    archiveMetadata
        .setFreeFlexibleSegmentUnitCount(archiveMetadataThrift.getFreeFlexibleSegmentUnitCount());
    archiveMetadata.setUsedSpace(archiveMetadataThrift.getLogicalUsedSpace());
    archiveMetadata.setOverloaded(archiveMetadataThrift.isOverloaded());
    if (archiveMetadataThrift.isSetStoragePoolId()) {
      archiveMetadata.setStoragePoolId(archiveMetadataThrift.getStoragePoolId());
    } else {
      archiveMetadata.setStoragePoolId(null);
    }
    archiveMetadata.setMigrationSpeed((int) archiveMetadataThrift.getMigrationSpeed());
    archiveMetadata.addAlreadyMigratedPage(archiveMetadataThrift.getAlreadyMigratedPage());
    archiveMetadata.addTotalPageToMigrate(archiveMetadataThrift.getTotalPageToMigrate());
    archiveMetadata.setMaxMigrationSpeed(archiveMetadataThrift.getMaxMigrationSpeed());
    if (archiveMetadataThrift.isSetMigrateFailedSegIdList()) {
      for (SegIdThrift segIdThrift : archiveMetadataThrift.getMigrateFailedSegIdList()) {
        archiveMetadata.addMigrateFailedSegId(buildSegIdFrom(segIdThrift));
      }
    }
    archiveMetadata.setWeight(archiveMetadataThrift.getWeight());
    archiveMetadata.setDataSizeMb(archiveMetadataThrift.getDataSizeMb());
    return archiveMetadata;
  }

  public static ArchiveMetadata buildOtherArchiveMetadataFrom(
      ArchiveMetadataThrift archiveMetadataThrift) {
    Validate.notNull(archiveMetadataThrift);

    ArchiveMetadata archiveMetadataBase = new ArchiveMetadata();

    archiveMetadataBase.setDeviceName(archiveMetadataThrift.getDevName());
    archiveMetadataBase.setArchiveId(archiveMetadataThrift.getArchiveId());
    archiveMetadataBase.setStatus(ArchiveStatus.valueOf(archiveMetadataThrift.getStatus().name()));
    archiveMetadataBase
        .setStorageType(StorageType.valueOf(archiveMetadataThrift.getStoragetype().name()));
    archiveMetadataBase.setArchiveType(ArchiveType.valueOf(archiveMetadataThrift.getType().name()));
    archiveMetadataBase.setCreatedBy(archiveMetadataThrift.getCreatedBy());
    archiveMetadataBase.setUpdatedBy(archiveMetadataThrift.getUpdatedBy());
    archiveMetadataBase.setCreatedTime(archiveMetadataThrift.getCreatedTime());
    archiveMetadataBase.setUpdatedTime(archiveMetadataThrift.getUpdatedTime());
    archiveMetadataBase.setSerialNumber(archiveMetadataThrift.getSerialNumber());
    archiveMetadataBase.setSlotNo(archiveMetadataThrift.getSlotNo());
    archiveMetadataBase.setLogicalSpace(archiveMetadataThrift.getLogicalSpace());
    return archiveMetadataBase;
  }

  public static Collection<InstanceId> convertFromLong(Collection<Long> instanceIdInLongs) {
    Collection<InstanceId> instanceIds = new HashSet<InstanceId>(instanceIdInLongs.size());
    for (Long id : instanceIdInLongs) {
      instanceIds.add(new InstanceId(id));
    }
    return instanceIds;
  }

  public static SegIdThrift buildThriftSegIdFrom(SegId segId) {
    return new SegIdThrift(segId.getVolumeId().getId(), segId.getIndex());
  }

  public static SegId buildSegIdFrom(SegIdThrift segIdThrift) {
    return new SegId(segIdThrift.getVolumeId(), segIdThrift.getSegmentIndex());
  }

  public static SegmentMembershipThrift buildThriftMembershipFrom(SegId segId,
      SegmentMembership membership) {
    return buildThriftMembershipFrom(segId, membership, 0);
  }

  public static SegmentMembershipThrift buildThriftMembershipFrom(long volumeId, int segIndex,
      SegmentMembership membership) {
    return buildThriftMembershipFrom(volumeId, segIndex, membership, 0);
  }

  public static SegmentMembershipThrift buildThriftMembershipFrom(long volumeId, int segIndex,
      SegmentMembership membership, int lease) {
    if (membership == null) {
      return null;
    }

    Set<Long> secondaries = new HashSet<>();
    for (InstanceId secondary : membership.getSecondaries()) {
      secondaries.add(secondary.getId());
    }

    Set<Long> arbiters = new HashSet<>();
    for (InstanceId arbiter : membership.getArbiters()) {
      arbiters.add(arbiter.getId());
    }

    SegmentMembershipThrift membershipOfThrift = new SegmentMembershipThrift(volumeId, segIndex,
        membership.getSegmentVersion().getEpoch(), membership.getSegmentVersion().getGeneration(),
        membership.getPrimary().getId(), secondaries, arbiters);
    Set<InstanceId> joiningSecondaries = membership.getJoiningSecondaries();
    if (joiningSecondaries != null && joiningSecondaries.size() > 0) {
      Set<Long> joiningSecondariesId = new HashSet<>();
      for (InstanceId joiningSecondary : membership.getJoiningSecondaries()) {
        joiningSecondariesId.add(joiningSecondary.getId());
      }
      membershipOfThrift.setJoiningSecondaries(joiningSecondariesId);
    }

    Set<InstanceId> inactiveSecondaries = membership.getInactiveSecondaries();
    if (inactiveSecondaries != null && inactiveSecondaries.size() > 0) {
      Set<Long> inactiveSecondariesId = new HashSet<>();
      for (InstanceId inactiveSecondary : membership.getInactiveSecondaries()) {
        inactiveSecondariesId.add(inactiveSecondary.getId());
      }
      membershipOfThrift.setInactiveSecondaries(inactiveSecondariesId);
    }

    if (lease > 0) {
      membershipOfThrift.setLease(lease);
    }

    if (membership.getTempPrimary() != null) {
      membershipOfThrift.setTempPrimary(membership.getTempPrimary().getId());
    }

    if (membership.getSecondaryCandidate() != null) {
      membershipOfThrift.setSecondaryCanaidate(membership.getSecondaryCandidate().getId());
    }

    if (membership.getPrimaryCandidate() != null) {
      membershipOfThrift.setPrimaryCandidate(membership.getPrimaryCandidate().getId());
    }

    return membershipOfThrift;
  }

  public static SegmentMembershipThrift buildThriftMembershipFrom(SegId segId,
      SegmentMembership membership,
      int lease) {
    Validate.notNull(segId);
    return buildThriftMembershipFrom(segId.getVolumeId().getId(), segId.getIndex(), membership,
        lease);
  }

  public static SegmentMembershipSwitchThrift buildThriftMembershipFromSwitch(Long volumeId,
      int segIndex,
      SegmentMembership membership, int lease, Set<Short> checkSet, Map<Long, Short> switchValue) {
    if (membership == null) {
      return null;
    }

    checkAndPutSwitchValue(volumeId, switchValue, checkSet);

    Long primaryId = membership.getPrimary().getId();
    checkAndPutSwitchValue(primaryId, switchValue, checkSet);

    Short instanceIdPrimary = switchValue.get(membership.getPrimary().getId());

    Set<Short> secondaries = new HashSet<>();
    for (InstanceId secondary : membership.getSecondaries()) {
      Long instanceId = secondary.getId();
      checkAndPutSwitchValue(instanceId, switchValue, checkSet);

      secondaries.add(switchValue.get(instanceId));
    }

    Set<Short> arbiters = new HashSet<>();
    for (InstanceId arbiter : membership.getArbiters()) {
      Long instanceId = arbiter.getId();
      checkAndPutSwitchValue(instanceId, switchValue, checkSet);
      arbiters.add(switchValue.get(instanceId));
    }
    Short volumeIdSwitch = switchValue.get(volumeId);

    SegmentMembershipSwitchThrift membershipSwitchThrift = new SegmentMembershipSwitchThrift(
        volumeIdSwitch,
        segIndex, membership.getSegmentVersion().getEpoch(),
        membership.getSegmentVersion().getGeneration(),
        instanceIdPrimary, secondaries, arbiters);
    Set<InstanceId> joiningSecondaries = membership.getJoiningSecondaries();
    if (joiningSecondaries != null && joiningSecondaries.size() > 0) {
      Set<Short> joiningSecondariesId = new HashSet<>();
      for (InstanceId joiningSecondary : membership.getJoiningSecondaries()) {
        Long instanceId = joiningSecondary.getId();
        checkAndPutSwitchValue(instanceId, switchValue, checkSet);

        joiningSecondariesId.add(switchValue.get(instanceId));
      }
      membershipSwitchThrift.setJoiningSecondariesSwitch(joiningSecondariesId);
    }

    Set<InstanceId> inactiveSecondaries = membership.getInactiveSecondaries();
    if (inactiveSecondaries != null && inactiveSecondaries.size() > 0) {
      Set<Short> inactiveSecondariesId = new HashSet<>();
      for (InstanceId inactiveSecondary : membership.getInactiveSecondaries()) {
        Long instanceId = inactiveSecondary.getId();
        checkAndPutSwitchValue(instanceId, switchValue, checkSet);

        inactiveSecondariesId.add(switchValue.get(instanceId));
      }
      membershipSwitchThrift.setInactiveSecondariesSwitch(inactiveSecondariesId);
    }

    if (lease > 0) {
      membershipSwitchThrift.setLease(lease);
    }

    if (membership.getTempPrimary() != null) {
      membershipSwitchThrift.setTempPrimary(membership.getTempPrimary().getId());
    }

    if (membership.getSecondaryCandidate() != null) {
      membershipSwitchThrift.setSecondaryCanaidate(membership.getSecondaryCandidate().getId());
    }

    if (membership.getPrimaryCandidate() != null) {
      membershipSwitchThrift.setPrimaryCandidate(membership.getPrimaryCandidate().getId());
    }

    return membershipSwitchThrift;
  }

  public static SegmentMembershipSwitchThrift buildThriftMembershipFromSwitch(SegId segId,
      SegmentMembership membership, int lease, Set<Short> checkSet, Map<Long, Short> switchValue) {
    Validate.notNull(segId);
    return buildThriftMembershipFromSwitch(segId.getVolumeId().getId(), segId.getIndex(),
        membership, lease,
        checkSet, switchValue);
  }

  public static SegmentUnitMetadataThrift buildThriftSegUnitMetadataFrom(
      SegmentUnitMetadata segUnitMetadata) {
    Validate.notNull(segUnitMetadata);
    SegId segId = segUnitMetadata.getSegId();

    SegmentMembershipThrift segmentMembershipThrift = RequestResponseHelper
        .buildThriftMembershipFrom(segId, segUnitMetadata.getMembership());
    SegmentUnitMetadataThrift segUnitMetadataThrift = new SegmentUnitMetadataThrift(
        segId.getVolumeId().getId(),
        segId.getIndex(), segUnitMetadata.getLogicalDataOffset(), segmentMembershipThrift,
        SegmentUnitStatusThrift.valueOf(segUnitMetadata.getStatus().name()),
        segUnitMetadata.getSegmentUnitType().getSegmentUnitTypeThrift(),
        segUnitMetadata.getLastUpdated(),
        segUnitMetadata.isEnableLaunchMultiDrivers(),
        buildThriftVolumeSource(segUnitMetadata.getVolumeSource()));
    segUnitMetadataThrift.setVolumeType(segUnitMetadata.getVolumeType().name());
    segUnitMetadataThrift.setAccountMetadataJson(segUnitMetadata.getAccountMetadataJson());
    segUnitMetadataThrift.setVolumeMetadataJson(segUnitMetadata.getVolumeMetadataJson());
    segUnitMetadataThrift.setDiskName(segUnitMetadata.getDiskName());
    segUnitMetadataThrift.setArchiveId(segUnitMetadata.getArchiveId());
    segUnitMetadataThrift.setMigrationSpeed(segUnitMetadata.getMigrationSpeed());
    segUnitMetadataThrift.setMinMigrationSpeed(segUnitMetadata.getMinMigrationSpeed());
    segUnitMetadataThrift.setMaxMigrationSpeed(segUnitMetadata.getMaxMigrationSpeed());
    segUnitMetadataThrift.setInnerMigrating(segUnitMetadata.isInnerMigrating());
    segUnitMetadataThrift.setSourceVolumeId(segUnitMetadata.getSrcVolumeId());

    long snapshotTotalCapacity = 0;
    Map<Integer, Long> snapshotMap = new HashMap<>();

    if (segUnitMetadata.getInstanceId() != null) {
      segUnitMetadataThrift.setInstanceId(segUnitMetadata.getInstanceId().getId());
    }

    double ratioFreePages = 0.0;
    if (segUnitMetadata.getPageCount() != 0) {
      ratioFreePages =
          (double) segUnitMetadata.getFreePageCount() / (double) segUnitMetadata.getPageCount();
    }

    if (ratioFreePages > 1) {
      logger.error("why is ratio larger than 1 ? {} {} {} {}", ratioFreePages,
          segUnitMetadata.getFreePageCount(),
          segUnitMetadata.getPageCount(), segUnitMetadata);
    }

    segUnitMetadataThrift.setRatioFreePages(ratioFreePages);
    if (segUnitMetadata.getInstanceId() != null) {
      segUnitMetadataThrift.setInstanceId(segUnitMetadata.getInstanceId().getId());
    }
    if (segUnitMetadata.getRatioMigration() != 0) {
      segUnitMetadataThrift.setRatioMigration(segUnitMetadata.getRatioMigration());
    }

    logger.debug("build segUnitMetadataThrift:{}", segUnitMetadataThrift);
    logger.debug("build segUnitMetadata:{}", segUnitMetadata);
    return segUnitMetadataThrift;
  }

  public static SegmentUnitMetadataSwitchThrift buildThriftSegUnitMetadataFromSwitch(
      SegmentUnitMetadata segUnitMetadata, Set<Short> checkSet, Map<Long, Short> switchValue) {
    Validate.notNull(segUnitMetadata);
    SegId segId = segUnitMetadata.getSegId();

    SegmentMembershipSwitchThrift segmentMembershipSwitchThrift = RequestResponseHelper
        .buildThriftMembershipFromSwitch(segId, segUnitMetadata.getMembership(), 0, checkSet,
            switchValue);

    Long volumeId = segId.getVolumeId().getId();
    checkAndPutSwitchValue(volumeId, switchValue, checkSet);

    SegmentUnitMetadataSwitchThrift metadataSwitchThrift =
        new SegmentUnitMetadataSwitchThrift(
            switchValue.get(volumeId), segId.getIndex(), segUnitMetadata.getLogicalDataOffset(),
            segmentMembershipSwitchThrift,
            SegmentUnitStatusThrift.valueOf(segUnitMetadata.getStatus().name()),
            segUnitMetadata.getSegmentUnitType().getSegmentUnitTypeThrift(),
            segUnitMetadata.getLastUpdated(),
            segUnitMetadata.isEnableLaunchMultiDrivers(),
            buildThriftVolumeSource(segUnitMetadata.getVolumeSource()));

    metadataSwitchThrift.setVolumeType(segUnitMetadata.getVolumeType().name());
    metadataSwitchThrift
        .setAccountMetadataJson(segUnitMetadata.getAccountMetadataJson());
    metadataSwitchThrift
        .setVolumeMetadataJson(segUnitMetadata.getVolumeMetadataJson());
    metadataSwitchThrift.setDiskName(segUnitMetadata.getDiskName());

    Long archiveId = segUnitMetadata.getArchiveId();
    checkAndPutSwitchValue(archiveId, switchValue, checkSet);

    metadataSwitchThrift.setArchiveIdSwitch(switchValue.get(archiveId));
    metadataSwitchThrift.setMigrationSpeed(segUnitMetadata.getMigrationSpeed());
    metadataSwitchThrift.setMinMigrationSpeed(segUnitMetadata.getMinMigrationSpeed());
    metadataSwitchThrift.setMaxMigrationSpeed(segUnitMetadata.getMaxMigrationSpeed());
    metadataSwitchThrift.setInnerMigrating(segUnitMetadata.isInnerMigrating());

    metadataSwitchThrift
        .setAlreadyMigratedPage(segUnitMetadata.getAlreadyMigratedPage());
    metadataSwitchThrift
        .setTotalPageToMigrate(segUnitMetadata.getTotalPageToMigrate());
    metadataSwitchThrift.setSourceVolumeId(segUnitMetadata.getSrcVolumeId());

    if (segUnitMetadata.getInstanceId() != null) {
      Long instanceId = segUnitMetadata.getInstanceId().getId();
      checkAndPutSwitchValue(instanceId, switchValue, checkSet);
      metadataSwitchThrift.setArchiveIdSwitch(switchValue.get(instanceId));
    }

    double ratioFreePages = 0.0;
    if (segUnitMetadata.getPageCount() != 0) {
      ratioFreePages =
          (double) segUnitMetadata.getFreePageCount() / (double) segUnitMetadata.getPageCount();
    }

    if (ratioFreePages > 1) {
      logger.error("why is ratio larger than 1 ? {} {} {} {}", ratioFreePages,
          segUnitMetadata.getFreePageCount(),
          segUnitMetadata.getPageCount(), segUnitMetadata);
    }

    metadataSwitchThrift.setRatioFreePages(ratioFreePages);
    if (segUnitMetadata.getInstanceId() != null) {
      Long instanceId = segUnitMetadata.getInstanceId().getId();

      checkAndPutSwitchValue(instanceId, switchValue, checkSet);
      metadataSwitchThrift.setInstanceIdSwitch(switchValue.get(instanceId));
    }

    if (segUnitMetadata.getRatioMigration() != 0) {
      metadataSwitchThrift.setRatioMigration(segUnitMetadata.getRatioMigration());
    }

    // for segment unit index = 0, report the snapshot meta data to information center;

    logger.debug("build metadataSwitchThrift:{}", metadataSwitchThrift);
    logger.debug("build segUnitMetadata:{}", segUnitMetadata);
    return metadataSwitchThrift;
  }

  public static SegmentUnitMetadata buildSegmentUnitMetadataFrom(
      SegmentUnitMetadataThrift segUnitMetaThrift) {
    Validate.notNull(segUnitMetaThrift);

    SegmentMembership membership = buildSegmentMembershipFrom(segUnitMetaThrift.getMembership())
        .getSecond();
    SegmentUnitStatus status = SegmentUnitStatus.valueOf(segUnitMetaThrift.getStatus().name());
    VolumeType volumeType = segUnitMetaThrift.getVolumeType() == null
        ? null : VolumeType.valueOf(segUnitMetaThrift.getVolumeType());

    SegId segId = new SegId(segUnitMetaThrift.getVolumeId(), segUnitMetaThrift.getSegIndex());
    SegmentUnitMetadata segUnit = new SegmentUnitMetadata(segId, segUnitMetaThrift.getOffset(),
        membership, status,
        volumeType, convertFromSegmentUnitTypeThrift(segUnitMetaThrift.getSegmentUnitType()));
    if (segUnitMetaThrift.isSetInstanceId()) {
      segUnit.setInstanceId(new InstanceId(segUnitMetaThrift.getInstanceId()));
      //check the Candidate
      if (membership.isPrimaryCandidate(segUnit.getInstanceId())
          || membership.isSecondaryCandidate(segUnit.getInstanceId())) {
        segUnit.setSecondaryCandidate(true);
      }
    }

    segUnit.setAccountMetadataJson(segUnitMetaThrift.getAccountMetadataJson());
    segUnit.setVolumeMetadataJson(segUnitMetaThrift.getVolumeMetadataJson());
    segUnit.setLastUpdated(segUnitMetaThrift.getLastUpdated());
    segUnit.setLastReported(System.currentTimeMillis());
    segUnit.setDiskName(segUnitMetaThrift.getDiskName());
    segUnit.setArchiveId(segUnitMetaThrift.getArchiveId());
    if (segUnitMetaThrift.isSetRatioMigration()) {
      segUnit.setRatioMigration(segUnitMetaThrift.getRatioMigration());
    }

    segUnit.setTotalPageToMigrate((int) segUnitMetaThrift.getTotalPageToMigrate());
    segUnit.setAlreadyMigratedPage((int) segUnitMetaThrift.getAlreadyMigratedPage());
    segUnit.setMigrationSpeed((int) segUnitMetaThrift.getMigrationSpeed());
    segUnit.setInnerMigrating(segUnitMetaThrift.isInnerMigrating());

    Map<Integer, AtomicLong> snapshotMap = new HashMap<>();

    segUnit.setEnableLaunchMultiDrivers(segUnitMetaThrift.isEnableLaunchMultiDrivers());
    segUnit.setVolumeSource(buildVolumeSourceTypeFrom(segUnitMetaThrift.getVolumeSource()));
    segUnit.setSrcVolumeId(segUnitMetaThrift.getSourceVolumeId());
    logger.debug("build segmentUnitMetadataThrift:{}", segUnitMetaThrift);
    logger.debug("build segUnitMetadata:{}", segUnit);
    return segUnit;
  }

  public static SegmentUnitMetadata buildSegmentUnitMetadataFromSwitch(
      SegmentUnitMetadataSwitchThrift segUnitMetaThrift, Map<Short, Long> switchValueReversal) {
    Validate.notNull(segUnitMetaThrift);

    SegmentMembership membership = buildSegmentMembershipFromSwitch(
        segUnitMetaThrift.getMembership(),
        switchValueReversal).getSecond();
    SegmentUnitStatus status = SegmentUnitStatus.valueOf(segUnitMetaThrift.getStatus().name());
    VolumeType volumeType = segUnitMetaThrift.getVolumeType() == null
        ? null : VolumeType.valueOf(segUnitMetaThrift.getVolumeType());

    long volumeId = switchValueReversal.get(segUnitMetaThrift.getVolumeIdSwitch());
    SegId segId = new SegId(volumeId, segUnitMetaThrift.getSegIndex());

    SegmentUnitMetadata segUnit = new SegmentUnitMetadata(segId, segUnitMetaThrift.getOffset(),
        membership, status,
        volumeType, convertFromSegmentUnitTypeThrift(segUnitMetaThrift.getSegmentUnitType()));
    if (segUnitMetaThrift.isSetInstanceIdSwitch()) {
      Short idSwitch = segUnitMetaThrift.getInstanceIdSwitch();
      long instanceId = switchValueReversal.get(idSwitch);
      segUnit.setInstanceId(new InstanceId(instanceId));
    }
    segUnit.setAccountMetadataJson(segUnitMetaThrift.getAccountMetadataJson());
    segUnit.setVolumeMetadataJson(segUnitMetaThrift.getVolumeMetadataJson());
    segUnit.setLastUpdated(segUnitMetaThrift.getLastUpdated());
    segUnit.setLastReported(System.currentTimeMillis());
    segUnit.setDiskName(segUnitMetaThrift.getDiskName());

    Short archiveIdSwitch = segUnitMetaThrift.getArchiveIdSwitch();
    long archiveId = switchValueReversal.get(archiveIdSwitch);
    segUnit.setArchiveId(archiveId);
    if (segUnitMetaThrift.isSetRatioMigration()) {
      segUnit.setRatioMigration(segUnitMetaThrift.getRatioMigration());
    }

    segUnit.setTotalPageToMigrate((int) segUnitMetaThrift.getTotalPageToMigrate());
    segUnit.setAlreadyMigratedPage((int) segUnitMetaThrift.getAlreadyMigratedPage());
    segUnit.setMigrationSpeed((int) segUnitMetaThrift.getMigrationSpeed());
    segUnit.setInnerMigrating(segUnitMetaThrift.isInnerMigrating());

    segUnit.setEnableLaunchMultiDrivers(segUnitMetaThrift.isEnableLaunchMultiDrivers());
    segUnit.setVolumeSource(buildVolumeSourceTypeFrom(segUnitMetaThrift.getVolumeSource()));
    segUnit.setSrcVolumeId(segUnitMetaThrift.getSourceVolumeId());
    return segUnit;
  }

  public static SegmentUnitStatus convertSegmentUnitStatus(SegmentUnitStatusThrift status) {
    Validate.notNull(status);
    return Enum.valueOf(SegmentUnitStatus.class, status.name());
  }

  public static IoLimitation.LimitType convertLimitType(LimitTypeThrift limitType) {
    Validate.notNull(limitType);
    return Enum.valueOf(IoLimitation.LimitType.class, limitType.name());
  }

  public static LimitTypeThrift convertLimitType(IoLimitation.LimitType limitType) {
    Validate.notNull(limitType);
    return Enum.valueOf(LimitTypeThrift.class, limitType.name());
  }

  public static VolumeType convertVolumeType(VolumeTypeThrift volumeType) {
    Validate.notNull(volumeType);
    return Enum.valueOf(VolumeType.class, volumeType.name());
  }

  public static VolumeStatus convertVolumeStatus(VolumeStatusThrift volumeStatus) {
    Validate.notNull(volumeStatus);
    return Enum.valueOf(VolumeStatus.class, volumeStatus.name());
  }

  public static Pair<SegId, SegmentMembership> buildSegmentMembershipFrom(
      SegmentMembershipThrift thriftMembership) {
    Validate.notNull(thriftMembership);

    Collection<InstanceId> secondaries = null;
    if (thriftMembership.getSecondaries() != null) {
      secondaries = convertFromLong(thriftMembership.getSecondaries());
    }

    Collection<InstanceId> arbiters = null;
    if (thriftMembership.getArbiters() != null) {
      arbiters = convertFromLong(thriftMembership.getArbiters());
    }

    Collection<InstanceId> inactiveSecondaries = null;
    if (thriftMembership.getInactiveSecondaries() != null) {
      inactiveSecondaries = convertFromLong(thriftMembership.getInactiveSecondaries());
    }

    Collection<InstanceId> joiningSecondaries = null;
    if (thriftMembership.getJoiningSecondaries() != null) {
      joiningSecondaries = convertFromLong(thriftMembership.getJoiningSecondaries());
    }

    InstanceId tempPrimary = null;
    if (thriftMembership.isSetTempPrimary() && thriftMembership.getTempPrimary() != 0) {
      tempPrimary = new InstanceId(thriftMembership.getTempPrimary());
    }

    InstanceId secondaryCandidate = null;
    if (thriftMembership.isSetSecondaryCanaidate()
        && thriftMembership.getSecondaryCanaidate() != 0) {
      secondaryCandidate = new InstanceId(thriftMembership.getSecondaryCanaidate());
    }

    InstanceId primaryCandidate = null;
    if (thriftMembership.isSetPrimaryCandidate() && thriftMembership.getPrimaryCandidate() != 0) {
      primaryCandidate = new InstanceId(thriftMembership.getPrimaryCandidate());
    }
    SegmentMembership segMembership;

    segMembership = new SegmentMembership(
        new SegmentVersion(thriftMembership.getEpoch(), thriftMembership.getGeneration()),
        new InstanceId(thriftMembership.getPrimary()), tempPrimary, secondaries, arbiters,
        inactiveSecondaries,
        joiningSecondaries, secondaryCandidate, primaryCandidate);
    return new Pair<>(new SegId(thriftMembership.getVolumeId(), thriftMembership.getSegIndex()),
        segMembership);
  }

  public static Pair<SegId, SegmentMembership> buildSegmentMembershipFromSwitch(
      SegmentMembershipSwitchThrift thriftMembership, Map<Short, Long> switchValueReversal) {
    Validate.notNull(thriftMembership);

    Collection<InstanceId> secondaries = null;
    if (thriftMembership.getSecondariesSwitch() != null) {
      Set<Long> secondarieSwitch = new HashSet<>();
      for (Short value : thriftMembership.getSecondariesSwitch()) {
        secondarieSwitch.add(switchValueReversal.get(value));
      }
      secondaries = convertFromLong(secondarieSwitch);
    }

    Collection<InstanceId> arbiters = null;
    if (thriftMembership.getArbitersSwitch() != null) {
      Set<Long> arbiterSwitch = new HashSet<>();
      for (Short value : thriftMembership.getArbitersSwitch()) {
        arbiterSwitch.add(switchValueReversal.get(value));
      }
      arbiters = convertFromLong(arbiterSwitch);
    }

    Collection<InstanceId> inactiveSecondaries = null;
    if (thriftMembership.getInactiveSecondariesSwitch() != null) {
      Set<Long> inactiveSecondariesSwitch = new HashSet<>();
      for (Short value : thriftMembership.getInactiveSecondariesSwitch()) {
        inactiveSecondariesSwitch.add(switchValueReversal.get(value));
      }

      inactiveSecondaries = convertFromLong(inactiveSecondariesSwitch);
    }

    Collection<InstanceId> joiningSecondaries = null;
    if (thriftMembership.getJoiningSecondariesSwitch() != null) {
      Set<Long> joiningSecondariesSwitch = new HashSet<>();
      for (Short value : thriftMembership.getJoiningSecondariesSwitch()) {
        joiningSecondariesSwitch.add(switchValueReversal.get(value));
      }

      joiningSecondaries = convertFromLong(joiningSecondariesSwitch);
    }

    InstanceId tempPrimary = null;
    if (thriftMembership.isSetTempPrimary() && thriftMembership.getTempPrimary() != 0) {
      tempPrimary = new InstanceId(thriftMembership.getTempPrimary());
    }

    InstanceId secondaryCandidate = null;
    if (thriftMembership.isSetSecondaryCanaidate()
        && thriftMembership.getSecondaryCanaidate() != 0) {
      secondaryCandidate = new InstanceId(thriftMembership.getSecondaryCanaidate());
    }

    InstanceId primaryCandidate = null;
    if (thriftMembership.isSetPrimaryCandidate() && thriftMembership.getPrimaryCandidate() != 0) {
      primaryCandidate = new InstanceId(thriftMembership.getPrimaryCandidate());
    }

    long primarySwitch = switchValueReversal.get(thriftMembership.getPrimarySwitch());
    SegmentMembership segMembership;

    segMembership = new SegmentMembership(
        new SegmentVersion(thriftMembership.getEpoch(), thriftMembership.getGeneration()),
        new InstanceId(primarySwitch), tempPrimary, secondaries, arbiters, inactiveSecondaries,
        joiningSecondaries, secondaryCandidate, primaryCandidate);
    long volumeId = switchValueReversal.get(thriftMembership.getVolumeIdSwitch());

    return new Pair<>(new SegId(volumeId, thriftMembership.getSegIndex()), segMembership);
  }

  public static List<EndPoint> buildEndPoints(InstanceStore instanceStore,
      SegmentMembership membership,
      boolean aliveMembersOnly, InstanceId... instancesExcluded) {
    return buildEndPoints(instanceStore, membership, PortType.CONTROL, aliveMembersOnly,
        instancesExcluded);
  }

  /**
   * Build endpoints from a given membership. Instances, the id of which are in the list, will be
   * excluded.
   *
   * @param instancesExcluded If it is true, exclude my endpoint in the returned list
   */
  public static List<EndPoint> buildEndPoints(InstanceStore instanceStore,
      SegmentMembership membership,
      PortType serviceNameEnumValue, boolean aliveMembersOnly, InstanceId... instancesExcluded) {
    Validate.notNull(instanceStore);
    Validate.notNull(membership);
    // Primary could be null when the segment is being initiated

    List<InstanceId> members = new ArrayList<>(membership.getAliveSecondaries().size() + 1);

    if (membership.getPrimary() != null) {
      members.add(membership.getPrimary());
    }

    members.addAll(membership.getAliveSecondaries());
    if (!aliveMembersOnly) {
      members.addAll(membership.getInactiveSecondaries());
    }
    Set<InstanceId> excludedInstanceSet = new HashSet<>();
    if (instancesExcluded != null) {
      for (InstanceId excludedInstance : instancesExcluded) {
        excludedInstanceSet.add(excludedInstance);
      }
    }

    List<EndPoint> endpoints = new ArrayList<>(members.size());
    for (InstanceId instanceId : members) {
      if (excludedInstanceSet.size() > 0 && excludedInstanceSet.contains(instanceId)) {
        continue;
      }

      Instance instance = instanceStore.get(instanceId);
      if (instance != null) {
        endpoints.add(instance.getEndPointByServiceName(serviceNameEnumValue));
      } else {
        logger.warn("Can't get instance {} from instance store", instanceId);
      }
    }
    return endpoints;
  }

  public static List<EndPoint> buildEndPoints(InstanceStore instanceStore,
      Collection<InstanceId> instanceIds) {
    List<EndPoint> endPoints = new ArrayList<>();
    if (instanceIds != null) {
      for (InstanceId instanceId : instanceIds) {
        Instance instance = instanceStore.get(instanceId);
        if (instance != null) {
          endPoints.add(instance.getEndPoint());
        } else {
          logger.warn("Can't get instance {} from instance store", instanceId);
        }
      }
    }
    return endPoints;
  }

  public static List<EndPoint> buildSecondariesEndPoint(InstanceStore instanceStore,
      SegmentMembership membership) {
    List<EndPoint> endPoints = new ArrayList<>();
    Set<InstanceId> instanceIds = membership.getSecondaries();
    if (instanceIds != null) {
      for (InstanceId instanceId : instanceIds) {
        Instance instance = instanceStore.get(instanceId);
        if (instance != null) {
          endPoints.add(instance.getEndPoint());
        } else {
          logger.warn("Can't get instance {} from instance store", instanceId);
        }
      }
    }
    return endPoints;
  }

  public static List<EndPoint> buildJoiningSecondariesEndPoint(InstanceStore instanceStore,
      SegmentMembership membership) {
    List<EndPoint> endPoints = new ArrayList<>();
    Set<InstanceId> instanceIds = membership.getJoiningSecondaries();
    if (instanceIds != null) {
      for (InstanceId instanceId : instanceIds) {
        Instance instance = instanceStore.get(instanceId);
        if (instance != null) {
          endPoints.add(instance.getEndPoint());
        } else {
          logger.warn("Can't get instance {} from instance store", instanceId);
        }
      }
    }
    return endPoints;
  }

  public static EndPoint buildPrimaryEndPoint(InstanceStore instanceStore,
      SegmentMembership membership) {
    Instance instance = instanceStore.get(membership.getPrimary());
    EndPoint primaryEndPoint = null;
    if (instance != null) {
      primaryEndPoint = instance.getEndPointByServiceName(PortType.CONTROL);
    } else {
      logger.error("Can't get instance {} from instance store", membership.getPrimary());
    }
    return primaryEndPoint;
  }

  public static DeleteSegmentUnitRequest buildDeleteSegmentUnitRequest(SegId segId,
      SegmentMembership currentMembership) {
    DeleteSegmentUnitRequest request = new DeleteSegmentUnitRequest(RequestIdBuilder.get(),
        segId.getVolumeId().getId(), segId.getIndex(),
        RequestResponseHelper.buildThriftMembershipFrom(segId, currentMembership));
    return request;
  }

  public static DeleteSegmentRequest buildDeleteSegmentRequest(SegId segId,
      SegmentMembership currentMembership) {
    DeleteSegmentRequest request = new DeleteSegmentRequest(RequestIdBuilder.get(),
        segId.getVolumeId().getId(),
        segId.getIndex(),
        RequestResponseHelper.buildThriftMembershipFrom(segId, currentMembership));
    return request;
  }

  /**
   * NOTE: This function should be only used to update volumeMetadataJson rather than updating
   * segment membership and segment unit's status.
   */
  public static UpdateSegmentUnitVolumeMetadataJsonRequest
      buildUpdateSegmentUnitVolumeMetadataJsonRequest(
      SegId segId, SegmentMembership currentMembership, String volumeMetadataJson) {
    SegmentMembershipThrift membershipThrift = RequestResponseHelper
        .buildThriftMembershipFrom(segId, currentMembership);
    UpdateSegmentUnitVolumeMetadataJsonRequest request =
        new UpdateSegmentUnitVolumeMetadataJsonRequest(
            RequestIdBuilder.get(), segId.getVolumeId().getId(), segId.getIndex(), membershipThrift,
            volumeMetadataJson);
    return request;
  }

  public static CreateSegmentUnitRequest buildCreateSegmentUnitRequest(SegId segId,
      VolumeMetadata volumeMetadata,
      AccountMetadata accountMetadata, Long primary, Long storagePoolId,
      SegmentUnitType segmentUnitType,
      Long... members) throws JsonProcessingException {
    List<Long> listMembers = new ArrayList<>();
    listMembers.add(primary);
    if (members != null) {
      for (Long member : members) {
        listMembers.add(member);
      }
    }
    return buildCreateSegmentUnitRequest(segId, volumeMetadata, accountMetadata, storagePoolId,
        segmentUnitType,
        listMembers);
  }

  public static CreateSegmentUnitRequest buildCreateSegmentUnitRequest(SegId segId,
      VolumeType volumeType,
      CacheType cacheType, Long primary, Long storagePoolId,
      SegmentUnitType segmentUnitType,
      Long... members) throws JsonProcessingException {
    List<Long> listMembers = new ArrayList<>();
    listMembers.add(primary);
    if (members != null) {
      for (Long member : members) {
        listMembers.add(member);
      }
    }
    return buildCreateSegmentUnitRequest(segId, volumeType, storagePoolId,
        segmentUnitType,
        listMembers);
  }

  public static CreateSegmentUnitRequest buildCreateSegmentUnitRequest(SegId segId,
      VolumeMetadata volumeMetadata,
      AccountMetadata accountMetadata, Long storagePoolId, SegmentUnitType segmentUnitType,
      List<Long> members)
      throws JsonProcessingException {
    if (members == null) {
      logger.warn(
          "Can't create a CreateSegmentUnitRequest because the number of members in the request "
              + "is null");
      throw new RuntimeException();
    }
    if (members.size() < 2) {
      logger.warn("the number of members in the request is less than 2");
    }

    InstanceId primaryId = new InstanceId(members.get(0));
    List<InstanceId> secondaries = new ArrayList<InstanceId>();
    List<InstanceId> arbiters = new ArrayList<InstanceId>();
    for (int i = 1; i < members.size(); i++) {
      if (i - 1 < volumeMetadata.getVolumeType().getNumSecondaries()) {
        secondaries.add(new InstanceId(members.get(i)));
      } else {
        arbiters.add(new InstanceId(members.get(i)));
      }
    }

    SegmentMembership membership = new SegmentMembership(primaryId, secondaries, arbiters);
    return buildCreateSegmentUnitRequest(segId, membership, 10, volumeMetadata, accountMetadata,
        storagePoolId,
        segmentUnitType);
  }

  public static CreateSegmentUnitRequest buildCreateSegmentUnitRequest(SegId segId,
      VolumeType volumeType, Long storagePoolId, SegmentUnitType segmentUnitType,
      List<Long> members) throws JsonProcessingException {
    if (members == null) {
      logger.warn(
          "Can't create a CreateSegmentUnitRequest because the number of members in the request "
              + "is null");
      throw new RuntimeException();
    }
    if (members.size() < 2) {
      logger.warn("the number of members in the request is less than 2");
    }

    InstanceId primaryId = new InstanceId(members.get(0));
    List<InstanceId> secondaries = new ArrayList<>();
    List<InstanceId> arbiters = new ArrayList<InstanceId>();
    for (int i = 1; i < members.size(); i++) {
      if (i - 1 < volumeType.getNumSecondaries()) {
        secondaries.add(new InstanceId(members.get(i)));
      } else {
        arbiters.add(new InstanceId(members.get(i)));
      }
    }

    SegmentMembership membership = new SegmentMembership(primaryId, secondaries, arbiters);
    return buildCreateSegmentUnitRequest(segId, membership, 10, volumeType,
        storagePoolId,
        segmentUnitType);
  }

  public static CreateSegmentUnitRequest buildCreateSegmentUnitRequest(SegId segId,
      SegmentMembership membership,
      int segmentWrapSize, VolumeType volumeType,
      Long storagePoolId,
      SegmentUnitType segmentUnitType) {
    // TODO: need datanode pass parameter enableLaunchMultiDrivers, and volume source
    CreateSegmentUnitRequest request = new CreateSegmentUnitRequest(RequestIdBuilder.get(),
        segId.getVolumeId().getId(), segId.getIndex(), volumeType.getVolumeTypeThrift(),
        storagePoolId, segmentUnitType.getSegmentUnitTypeThrift(),
        segmentWrapSize, false, CREATE)
        .setInitMembership(buildThriftMembershipFrom(segId, membership));
    return request;
  }

  public static CreateSegmentUnitRequest buildCreateSegmentUnitRequest(SegId segId,
      SegmentMembership membership,
      int segmentWrapSize, VolumeMetadata volumeMetadata, AccountMetadata accountMetadata,
      Long storagePoolId,
      SegmentUnitType segmentUnitType) throws JsonProcessingException {
    CreateSegmentUnitRequest request = new CreateSegmentUnitRequest(RequestIdBuilder.get(),
        segId.getVolumeId().getId(), segId.getIndex(),
        volumeMetadata.getVolumeType().getVolumeTypeThrift(), storagePoolId,
        segmentUnitType.getSegmentUnitTypeThrift(), segmentWrapSize,
        volumeMetadata.isEnableLaunchMultiDrivers(),
        buildThriftVolumeSource(volumeMetadata.getVolumeSource()))
        .setInitMembership(buildThriftMembershipFrom(segId, membership));
    ObjectMapper mapper = new ObjectMapper();
    String volumeMetadataJson = mapper.writeValueAsString(volumeMetadata);
    request.setVolumeMetadataJson(volumeMetadataJson);

    if (accountMetadata != null) {
      String accountMetadataJson = mapper.writeValueAsString(accountMetadata);
      request.setAccountMetadataJson(accountMetadataJson);
    }

    return request;
  }

  public static SegmentMembershipThrift getSegmentMembershipFromThrowable(Throwable t) {
    Class<?> throwableClass = t.getClass();

    Method[] methods = throwableClass.getMethods();
    SegmentMembershipThrift membership = null;
    for (Method method : methods) {
      if (method.getReturnType().equals(SegmentMembershipThrift.class)) {
        try {
          membership = (SegmentMembershipThrift) (method.invoke(t, (Object[]) null));
        } catch (Exception e) {
          logger.warn("Can't get segment membership from the exception: {}", t.toString(), e);
        }
      }
    }
    return membership;
  }

  public static ReadRequest buildReadDataRequest(long volumeId, int segIndex,
      List<ReadRequestUnit> readRequestUnits,
      SegmentMembership membership) {
    return buildReadDataRequest(RequestIdBuilder.get(), volumeId, segIndex, readRequestUnits,
        membership);
  }

  public static ReadRequest buildReadDataRequest(long requestId, long volumeId, int segIndex,
      List<ReadRequestUnit> readRequestUnits, SegmentMembership membership) {
    ReadRequest readRequest = new ReadRequest(requestId, volumeId, segIndex, readRequestUnits);
    if (membership != null) {
      SegmentMembershipThrift membershipThrift = buildThriftMembershipFrom(
          new SegId(volumeId, segIndex),
          membership);
      readRequest.setMembership(membershipThrift);
    }
    return readRequest;
  }

  public static WriteRequest buildWriteDataRequest(long requestId, long volumeId, int segIndex,
      List<WriteRequestUnit> writeRequestUnits, SegmentMembership membership, int snapshotVersion) {
    WriteRequest writeRequest = new WriteRequest(requestId, volumeId, segIndex, writeRequestUnits,
        snapshotVersion);
    if (membership != null) {
      SegmentMembershipThrift membershipThrift = buildThriftMembershipFrom(
          new SegId(volumeId, segIndex),
          membership);
      writeRequest.setMembership(membershipThrift);
    }
    return writeRequest;
  }

  public static WriteRequest buildWriteDataRequest(long volumeId, int segIndex,
      List<WriteRequestUnit> writeRequestUnits, SegmentMembership membership) {
    return buildWriteDataRequest(RequestIdBuilder.get(), volumeId, segIndex, writeRequestUnits,
        membership, 0);
  }

  public static boolean checksumMatch(byte[] data, long checksum) {
    return true;
  }

  public static BroadcastRequest buildGiveMeYourMembershipRequest(SegId segId,
      SegmentMembership membership,
      long myInstanceId) {
    long requestId = RequestIdBuilder.get();
    return buildGiveMeYourMembershipRequest(requestId, segId, membership, myInstanceId);
  }

  public static BroadcastRequest buildGiveMeYourMembershipRequest(long requestId, SegId segId,
      SegmentMembership membership, long myInstanceId) {
    BroadcastRequest request = new BroadcastRequest(requestId, 1, segId.getVolumeId().getId(),
        segId.getIndex(),
        BroadcastTypeThrift.GiveMeYourMembership,
        RequestResponseHelper.buildThriftMembershipFrom(segId, membership));
    request.setMyself(myInstanceId);
    return request;
  }

  public static BroadcastRequest buildBroadcastLogResultsRequest(SegId segId,
      SegmentMembership membership,
      long myInstanceId, CommitLogsRequestThrift commitLogsRequest) {
    BroadcastRequest request = new BroadcastRequest(RequestIdBuilder.get(), 1,
        segId.getVolumeId().getId(),
        segId.getIndex(), BroadcastTypeThrift.BroadcastLogResults,
        RequestResponseHelper.buildThriftMembershipFrom(segId, membership));
    request.setMyself(myInstanceId);
    request.setCommitLogsRequest(commitLogsRequest);
    return request;
  }

  public static DepartFromMembershipRequest buildDepartFromMembershipRequest(SegId segId,
      SegmentMembership membership, long myInstanceId) {
    long requestId = RequestIdBuilder.get();
    return new DepartFromMembershipRequest(requestId, segId.getVolumeId().getId(), segId.getIndex(),
        buildThriftMembershipFrom(segId, membership), myInstanceId);
  }

  public static DriverClientInfoThrift buildThriftDriverMetadataFrom(
      DriverClientInformation driverClientInformation) {
    DriverClientInfoThrift driverClientInfoThrift = new DriverClientInfoThrift();
    DriverClientKeyInformation driverClientKeyInformation = driverClientInformation
        .getDriverClientKeyInformation();
    driverClientInfoThrift.setDriverContainerId(driverClientKeyInformation.getDriverContainerId());
    driverClientInfoThrift.setVolumeId(driverClientKeyInformation.getVolumeId());
    driverClientInfoThrift.setSnapshotId(driverClientKeyInformation.getSnapshotId());
    driverClientInfoThrift
        .setDriverType(DriverTypeThrift.valueOf(driverClientKeyInformation.getDriverType()));
    driverClientInfoThrift.setClientInfo(driverClientKeyInformation.getClientInfo());
    driverClientInfoThrift.setTime(driverClientKeyInformation.getTime());

    driverClientInfoThrift.setDriverName(driverClientInformation.getDriverName());
    driverClientInfoThrift.setHostName(driverClientInformation.getHostName());
    driverClientInfoThrift.setStatus(driverClientInformation.isStatus());
    driverClientInfoThrift.setVolumeName(driverClientInformation.getVolumeName());
    driverClientInfoThrift.setVolumeDescription(driverClientInformation.getVolumeDescription());
    return driverClientInfoThrift;
  }

  public static DriverMetadataThrift buildThriftDriverMetadataFrom(DriverMetadata driverMetadata) {
    DriverMetadataThrift driverMetadataThrift = new DriverMetadataThrift();
    driverMetadataThrift.setDriverContainerId(driverMetadata.getDriverContainerId());
    driverMetadataThrift.setInstanceId(driverMetadata.getInstanceId());
    driverMetadataThrift.setVolumeId(driverMetadata.getVolumeId());
    driverMetadataThrift.setVolumeName(driverMetadata.getVolumeName());
    driverMetadataThrift.setSnapshotId(driverMetadata.getSnapshotId());
    driverMetadataThrift.setHostName(driverMetadata.getHostName());
    driverMetadataThrift.setProcessId(driverMetadata.getProcessId());
    driverMetadataThrift.setPort(driverMetadata.getPort());
    driverMetadataThrift
        .setDriverType(DriverTypeThrift.valueOf(driverMetadata.getDriverType().name()));
    driverMetadataThrift.setNbdDevice(driverMetadata.getNbdDevice());
    driverMetadataThrift.setProcessId(driverMetadata.getProcessId());
    driverMetadataThrift.setQueryServerIp(driverMetadata.getQueryServerIp());
    driverMetadataThrift.setQueryServerPort(driverMetadata.getQueryServerPort());
    driverMetadataThrift.setDriverName(driverMetadata.getDriverName());
    driverMetadataThrift
        .setDriverStatus(DriverStatusThrift.valueOf(driverMetadata.getDriverStatus().name()));
    driverMetadataThrift.setCoordinatorPort(driverMetadata.getCoordinatorPort());
    driverMetadataThrift
        .setPortalType(PortalTypeThrift.findByValue(driverMetadata.getPortalType().getValue()));
    driverMetadataThrift.setIpv6Addr(driverMetadata.getIpv6Addr());
    driverMetadataThrift.setNetIfaceName(driverMetadata.getNicName());

    Map<String, AccessPermissionTypeThrift> clientHostAccessRules = new HashMap<String,
        AccessPermissionTypeThrift>();
    if (driverMetadata.getClientHostAccessRule() != null && !driverMetadata
        .getClientHostAccessRule().isEmpty()) {
      for (Entry<String, AccessPermissionType> entry : driverMetadata.getClientHostAccessRule()
          .entrySet()) {
        if (entry == null) {
          continue;
        }
        clientHostAccessRules
            .put(entry.getKey(), AccessPermissionTypeThrift.valueOf(entry.getValue().name()));
      }
    }

    driverMetadataThrift.setStaticIoLimitationId(driverMetadata.getStaticIoLimitationId());
    driverMetadataThrift.setDynamicIoLimitationId(driverMetadata.getDynamicIoLimitationId());
    driverMetadataThrift.setClientHostAccessRule(clientHostAccessRules);
    driverMetadataThrift.setChapControl(driverMetadata.getChapControl());
    driverMetadataThrift.setCreateTime(driverMetadata.getCreateTime());
    driverMetadataThrift.setMakeUnmountForCsi(driverMetadata.isMakeUnmountForCsi());

    return driverMetadataThrift;
  }

  public static DriverMetadata buildDriverMetadataFrom(DriverMetadataThrift driverMetadataThrift) {
    DriverMetadata driverMetadata = new DriverMetadata();
    driverMetadata.setDriverContainerId(driverMetadataThrift.getDriverContainerId());
    driverMetadata.setInstanceId(driverMetadataThrift.getInstanceId());
    driverMetadata.setVolumeId(driverMetadataThrift.getVolumeId());
    driverMetadata.setVolumeName(driverMetadataThrift.getVolumeName());
    driverMetadata.setSnapshotId(driverMetadataThrift.getSnapshotId());
    driverMetadata.setDriverName(driverMetadataThrift.getDriverName());
    driverMetadata.setHostName(driverMetadataThrift.getHostName());
    driverMetadata.setProcessId(driverMetadataThrift.getProcessId());
    driverMetadata.setPort(driverMetadataThrift.getPort());
    driverMetadata.setDriverType(DriverType.valueOf(driverMetadataThrift.getDriverType().name()));
    driverMetadata
        .setDriverStatus(DriverStatus.valueOf(driverMetadataThrift.getDriverStatus().name()));
    driverMetadata.setCoordinatorPort(driverMetadataThrift.getCoordinatorPort());
    driverMetadata.setNbdDevice(driverMetadataThrift.getNbdDevice());
    driverMetadata.setProcessId(driverMetadataThrift.getProcessId());
    driverMetadata.setQueryServerIp(driverMetadataThrift.getQueryServerIp());
    driverMetadata.setQueryServerPort(driverMetadataThrift.getQueryServerPort());
    driverMetadata
        .setPortalType(PortalType.findByValue(driverMetadataThrift.getPortalType().getValue()));
    driverMetadata.setNicName(driverMetadataThrift.getNetIfaceName());
    driverMetadata.setIpv6Addr(driverMetadataThrift.getIpv6Addr());
    driverMetadata.setCreateTime(driverMetadataThrift.getCreateTime());
    Map<String, AccessPermissionType> clientHostAccessRules = new HashMap<String,
        AccessPermissionType>();
    if (driverMetadataThrift.getClientHostAccessRule() != null && !driverMetadataThrift
        .getClientHostAccessRule()
        .isEmpty()) {
      for (Entry<String, AccessPermissionTypeThrift> entry : driverMetadataThrift
          .getClientHostAccessRule()
          .entrySet()) {
        if (entry == null) {
          continue;
        }
        clientHostAccessRules
            .put(entry.getKey(), AccessPermissionType.valueOf(entry.getValue().name()));
      }
    }

    driverMetadata.setStaticIoLimitationId(driverMetadataThrift.getStaticIoLimitationId());
    driverMetadata.setDynamicIoLimitationId(driverMetadataThrift.getDynamicIoLimitationId());
    driverMetadata.setClientHostAccessRule(clientHostAccessRules);
    driverMetadata.setChapControl(driverMetadataThrift.getChapControl());
    driverMetadata.setCreateTime(driverMetadataThrift.getCreateTime());
    if (driverMetadataThrift.isSetMakeUnmountForCsi()) {
      driverMetadata.setMakeUnmountForCsi(driverMetadataThrift.isMakeUnmountForCsi());
    }
    return driverMetadata;
  }

  public static DriverClientInformation buildDriverMetadataFrom(
      DriverClientInfoThrift driverClientInfoThrift) {
    DriverClientInformation driverClientInformation = new DriverClientInformation();

    DriverClientKeyInformation driverClientKeyInformation = new DriverClientKeyInformation(
        driverClientInfoThrift.getDriverContainerId(),
        driverClientInfoThrift.getVolumeId(),
        driverClientInfoThrift.getSnapshotId(), driverClientInfoThrift.getDriverType().name(),
        driverClientInfoThrift.getClientInfo(), driverClientInfoThrift.getTime());

    driverClientInformation.setDriverClientKeyInformation(driverClientKeyInformation);
    driverClientInformation.setDriverName(driverClientInfoThrift.getDriverName());
    driverClientInformation.setHostName(driverClientInfoThrift.getHostName());
    driverClientInformation.setStatus(driverClientInfoThrift.isStatus());
    driverClientInformation.setVolumeName(driverClientInfoThrift.getVolumeName());
    driverClientInformation.setVolumeDescription(driverClientInfoThrift.getVolumeDescription());

    return driverClientInformation;
  }

  public static ScsiDriverMetadata buildScsiDriverMetadataFrom(
      ScsiDeviceInfoThrift scsiDeviceInfoThrift,
      long drivercontainerId) {
    ScsiDriverMetadata scsiDriverMetadata = new ScsiDriverMetadata();
    scsiDriverMetadata.setScsiDevice(scsiDeviceInfoThrift.getScsiDevice());
    scsiDriverMetadata.setScsiDeviceStatus(scsiDeviceInfoThrift.getScsiDeviceStatus().name());

    DriverKeyForScsi driverKeyForScsi = new DriverKeyForScsi();
    driverKeyForScsi.setDrivercontainerId(drivercontainerId);
    driverKeyForScsi.setVolumeId(scsiDeviceInfoThrift.getVolumeId());
    driverKeyForScsi.setSnapshotId(scsiDeviceInfoThrift.getSnapshotId());

    scsiDriverMetadata.setDriverKeyForScsi(driverKeyForScsi);
    return scsiDriverMetadata;
  }

  public static DriverContainerCandidate buildDriverContainerCandidateFrom(
      DriverContainerCandidateThrift driverContainerCandidataThrift) {
    DriverContainerCandidate driverContainerCandidate = new DriverContainerCandidate();
    driverContainerCandidate.setHostName(driverContainerCandidataThrift.getHostName());
    driverContainerCandidate.setPort(driverContainerCandidataThrift.getPort());
    driverContainerCandidate.setSequenceId(driverContainerCandidataThrift.getSequenceId());
    return driverContainerCandidate;
  }

  public static DriverContainerCandidateThrift buildThriftDriverContainerCandidata(
      DriverContainerCandidate driverContainerCandidate) {
    DriverContainerCandidateThrift driverContainerCandidataThrift =
        new DriverContainerCandidateThrift();
    driverContainerCandidataThrift.setHostName(driverContainerCandidate.getHostName());
    driverContainerCandidataThrift.setPort(driverContainerCandidate.getPort());
    driverContainerCandidataThrift.setSequenceId(driverContainerCandidate.getSequenceId());
    return driverContainerCandidataThrift;
  }

  public static SegmentMetadataThrift buildThriftSegmentMetadataFrom(SegmentMetadata segMetadata,
      boolean onlyIncludeLatestUnits) {
    SegmentMetadataThrift segmentMetadataThrift = new SegmentMetadataThrift();
    List<SegmentUnitMetadataThrift> segmentUnitsMetadataThrift = new ArrayList<>();
    for (Map.Entry<InstanceId, SegmentUnitMetadata> entry : segMetadata
        .getSegmentUnitMetadataTable().entrySet()) {
      SegmentUnitMetadata segUnitMetadata = entry.getValue();
      SegmentUnitMetadataThrift segmentUnitThrift = RequestResponseHelper
          .buildThriftSegUnitMetadataFrom(segUnitMetadata);
      segmentUnitThrift.setInstanceId(entry.getKey().getId());
      segmentUnitsMetadataThrift.add(segmentUnitThrift);
    }
    segmentMetadataThrift.setVolumeId(segMetadata.getSegId().getVolumeId().getId());
    segmentMetadataThrift.setSegId(segMetadata.getSegId().getIndex());
    segmentMetadataThrift.setIndexInVolume(segMetadata.getIndex());
    segmentMetadataThrift.setSegmentUnits(segmentUnitsMetadataThrift);
    segmentMetadataThrift.setFreeSpaceRatio(segMetadata.getFreeRatio());
    return segmentMetadataThrift;
  }

  public static SegmentMetadataSwitchThrift buildThriftSegmentMetadataFromSwitch(
      SegmentMetadata segMetadata,
      boolean onlyIncludeLatestUnits, Set<Short> checkSet, Map<Long, Short> switchValue) {
    SegmentMetadataSwitchThrift segmentMetadataSwitchThrift = new SegmentMetadataSwitchThrift();

    Long volumeId = segMetadata.getSegId().getVolumeId().getId();

    checkAndPutSwitchValue(volumeId, switchValue, checkSet);

    segmentMetadataSwitchThrift.setVolumeIdSwitch(switchValue.get(volumeId));

    List<SegmentUnitMetadataSwitchThrift> segmentUnitMetadataSwitchThrifts = new ArrayList<>();
    for (Map.Entry<InstanceId, SegmentUnitMetadata> entry : segMetadata
        .getSegmentUnitMetadataTable().entrySet()) {
      SegmentUnitMetadata segUnitMetadata = entry.getValue();
      SegmentUnitMetadataSwitchThrift segmentUnitMetadataSwitchThrift = RequestResponseHelper
          .buildThriftSegUnitMetadataFromSwitch(segUnitMetadata, checkSet, switchValue);

      Long instanceId = entry.getKey().getId();
      checkAndPutSwitchValue(instanceId, switchValue, checkSet);
      segmentUnitMetadataSwitchThrift.setInstanceIdSwitch(switchValue.get(instanceId));

      segmentUnitMetadataSwitchThrifts.add(segmentUnitMetadataSwitchThrift);
    }

    segmentMetadataSwitchThrift.setSegId(segMetadata.getSegId().getIndex());
    segmentMetadataSwitchThrift.setIndexInVolume(segMetadata.getIndex());
    segmentMetadataSwitchThrift.setSegmentUnits(segmentUnitMetadataSwitchThrifts);
    segmentMetadataSwitchThrift.setFreeSpaceRatio(segMetadata.getFreeRatio());
    return segmentMetadataSwitchThrift;
  }

  public static SegmentMetadata buildSegmentMetadataFrom(SegmentMetadataThrift segMetadataThrift) {
    SegmentMetadata segmentMetadata = new SegmentMetadata(
        new SegId(segMetadataThrift.getVolumeId(), segMetadataThrift.getSegId()),
        segMetadataThrift.getIndexInVolume());
    for (SegmentUnitMetadataThrift segUnitMetaThrift : segMetadataThrift.getSegmentUnits()) {
      SegmentUnitMetadata segmentUnit = RequestResponseHelper
          .buildSegmentUnitMetadataFrom(segUnitMetaThrift);
      segmentMetadata
          .putSegmentUnitMetadata(new InstanceId(segUnitMetaThrift.getInstanceId()), segmentUnit);
    }
    return segmentMetadata;
  }

  /**
   * just replace this method for compress volume meta data.
   */
  public static VolumeMetadataThrift buildThriftVolumeFrom1(VolumeMetadata volumeMetadata,
      boolean withSegmentList) {
    VolumeMetadataThrift volumeMetadataThrift = new VolumeMetadataThrift();
    volumeMetadataThrift.setVolumeId(volumeMetadata.getVolumeId());
    volumeMetadataThrift.setName(volumeMetadata.getName());
    volumeMetadataThrift.setVolumeSize(volumeMetadata.getVolumeSize());
    volumeMetadataThrift.setSegmentSize(volumeMetadata.getSegmentSize());
    volumeMetadataThrift.setVolumeType(volumeMetadata.getVolumeType().getVolumeTypeThrift());
    volumeMetadataThrift.setVolumeStatus(volumeMetadata.getVolumeStatus().getVolumeStatusThrift());
    volumeMetadataThrift.setAccountId(volumeMetadata.getAccountId());
    volumeMetadataThrift.setRootVolumeId(volumeMetadata.getRootVolumeId());
    volumeMetadataThrift.setExtendingSize(volumeMetadata.getExtendingSize());
    volumeMetadataThrift.setDeadTime(volumeMetadata.getDeadTime());
    volumeMetadataThrift
        .setLeastSegmentUnitAtBeginning(volumeMetadata.getSegmentNumToCreateEachTime());

    volumeMetadataThrift
        .setSegmentNumToCreateEachTime(volumeMetadata.getSegmentNumToCreateEachTime());
    volumeMetadataThrift.setVolumeLayout(volumeMetadata.getVolumeLayout());
    if (volumeMetadata.getDomainId() == null) {
      volumeMetadataThrift.setDomainId(0);
    } else {
      volumeMetadataThrift.setDomainId(volumeMetadata.getDomainId());
    }
    volumeMetadataThrift.setStoragePoolId(volumeMetadata.getStoragePoolId());

    List<SegmentMetadataThrift> segmentsMetadataThrift = new ArrayList<>();

    if (withSegmentList) {
      if (!volumeMetadata.getAddSegmentOrder().isEmpty()) {
        List<Integer> addSegmentOrder = volumeMetadata.getAddSegmentOrder();
        for (int i = 0; i < addSegmentOrder.size(); i++) {
          int segmentIndex = addSegmentOrder.get(i);
          SegmentMetadata segmentMetadata = volumeMetadata.getSegmentByIndex(segmentIndex);
          if (segmentMetadata.getSegmentUnitCount() > 0) {
            // if the number of segment unit in segment is greater than zero, add the segment
            segmentsMetadataThrift.add(buildThriftSegmentMetadataFrom(segmentMetadata, false));
          }
        }
      } else {
        for (SegmentMetadata entry : volumeMetadata.getSegments()) {
          if (entry.getSegmentUnitCount() > 0) {
            // if the number of segment unit in segment is greater than zero, add the segment
            segmentsMetadataThrift.add(buildThriftSegmentMetadataFrom(entry, false));
          }
        }
      }

      if (segmentsMetadataThrift.size() != volumeMetadata.getSegments().size()) {
        logger.warn("can not build enough segment thrift:{}, from volume:{}",
            segmentsMetadataThrift.size(),
            volumeMetadata.getSegments().size());
      }
    }
    volumeMetadataThrift.setSegmentsMetadata(segmentsMetadataThrift);
    volumeMetadataThrift.setFreeSpaceRatio(volumeMetadata.getFreeSpaceRatio());
    if (volumeMetadata.getVolumeCreatedTime() != null) {
      volumeMetadataThrift
          .setVolumeCreatedTime(volumeMetadata.getVolumeCreatedTime().getTime());
    }
    if (volumeMetadata.getLastExtendedTime() != null) {
      volumeMetadataThrift.setLastExtendedTime(volumeMetadata.getLastExtendedTime().getTime());
    }
    if (buildThriftVolumeSource(volumeMetadata.getVolumeSource()) != null) {
      volumeMetadataThrift
          .setVolumeSource(buildThriftVolumeSource(volumeMetadata.getVolumeSource()));
    }
    volumeMetadataThrift.setReadWrite(buildThriftReadWriteType(volumeMetadata.getReadWrite()));
    volumeMetadataThrift.setInAction(buildThriftVolumeInAction(volumeMetadata.getInAction()));
    volumeMetadataThrift.setPageWrappCount(volumeMetadata.getPageWrappCount());
    volumeMetadataThrift.setSegmentWrappCount(volumeMetadata.getSegmentWrappCount());
    volumeMetadataThrift.setEnableLaunchMultiDrivers(volumeMetadata.isEnableLaunchMultiDrivers());

    volumeMetadataThrift.setMigrationSpeed(volumeMetadata.getMigrationSpeed());
    volumeMetadataThrift.setMigrationRatio(volumeMetadata.getMigrationRatio());
    volumeMetadataThrift.setTotalPageToMigrate(volumeMetadata.getTotalPageToMigrate());
    volumeMetadataThrift.setAlreadyMigratedPage(volumeMetadata.getAlreadyMigratedPage());

    long volumeTotalMigrationPage = volumeMetadata.getTotalPageToMigrate();
    long volumeAlreadyMigrationPage = volumeMetadata.getAlreadyMigratedPage();
    volumeMetadataThrift.setMigrationRatio(
        0 == volumeTotalMigrationPage ? 100
            : (volumeAlreadyMigrationPage * 100) / volumeTotalMigrationPage);

    volumeMetadataThrift.setRebalanceRatio(volumeMetadata.getRebalanceInfo().getRebalanceRatio());
    volumeMetadataThrift
        .setRebalanceVersion(volumeMetadata.getRebalanceInfo().getRebalanceVersion());
    volumeMetadataThrift.setStableTime(volumeMetadata.getStableTime());
    volumeMetadataThrift.setMarkDelete(volumeMetadata.isMarkDelete());

    Map<Long, Short> switchValue = new HashMap<>();
    volumeMetadataThrift.setSwitchStructValue(switchValue);
    logger.debug("buildThriftVolumeFrom volumeMetadata info is {}", volumeMetadata);
    return volumeMetadataThrift;
  }

  public static void checkAndPutSwitchValue(Long value, Map<Long, Short> switchValue,
      Set<Short> checkSet) {
    // if we don't assert here, may be going to dead loop
    Validate.isTrue(checkSet.size() < SHORT_MAX_VALUE);
    if (!switchValue.containsKey(value)) {
      short shortKey = value.shortValue();
      while (checkSet.contains(shortKey)) {
        Integer randomInc = RandomUtils.nextInt(100);
        if (randomInc == 0) {
          randomInc++;
        }
        short shortInc = randomInc.shortValue();
        shortKey += shortInc;
      }
      Validate.isTrue(checkSet.add(shortKey));
      switchValue.put(value, shortKey);
    }
  }

  public static VolumeMetadataThrift buildThriftVolumeFrom(VolumeMetadata volumeMetadata,
      boolean withSegmentList) {
    Map<Long, Short> switchValue = new HashMap<>();
    Set<Short> checkSet = new HashSet<>();

    VolumeMetadataThrift volumeMetadataThrift = new VolumeMetadataThrift();
    volumeMetadataThrift.setVolumeId(volumeMetadata.getVolumeId());
    volumeMetadataThrift.setName(volumeMetadata.getName());
    volumeMetadataThrift.setVolumeSize(volumeMetadata.getVolumeSize());
    volumeMetadataThrift.setSegmentSize(volumeMetadata.getSegmentSize());
    volumeMetadataThrift.setVolumeType(volumeMetadata.getVolumeType().getVolumeTypeThrift());
    volumeMetadataThrift.setVolumeStatus(volumeMetadata.getVolumeStatus().getVolumeStatusThrift());
    volumeMetadataThrift.setAccountId(volumeMetadata.getAccountId());
    volumeMetadataThrift.setRootVolumeId(volumeMetadata.getRootVolumeId());
    volumeMetadataThrift.setExtendingSize(volumeMetadata.getExtendingSize());
    volumeMetadataThrift.setDeadTime(volumeMetadata.getDeadTime());
    volumeMetadataThrift
        .setLeastSegmentUnitAtBeginning(volumeMetadata.getSegmentNumToCreateEachTime());

    volumeMetadataThrift
        .setSegmentNumToCreateEachTime(volumeMetadata.getSegmentNumToCreateEachTime());
    volumeMetadataThrift.setVolumeLayout(volumeMetadata.getVolumeLayout());
    if (volumeMetadata.getDomainId() == null) {
      volumeMetadataThrift.setDomainId(0);
    } else {
      volumeMetadataThrift.setDomainId(volumeMetadata.getDomainId());
    }
    volumeMetadataThrift.setStoragePoolId(volumeMetadata.getStoragePoolId());

    List<SegmentMetadataSwitchThrift> segmentsMetadataThrift = new ArrayList<>();

    if (withSegmentList) {
      if (!volumeMetadata.getAddSegmentOrder().isEmpty()) {
        List<Integer> addSegmentOrder = volumeMetadata.getAddSegmentOrder();
        for (int i = 0; i < addSegmentOrder.size(); i++) {
          int segmentIndex = addSegmentOrder.get(i);
          SegmentMetadata segmentMetadata = volumeMetadata.getSegmentByIndex(segmentIndex);
          if (segmentMetadata.getSegmentUnitCount() > 0) {
            // if the number of segment unit in segment is greater than zero, add the segment
            segmentsMetadataThrift
                .add(buildThriftSegmentMetadataFromSwitch(segmentMetadata, false, checkSet,
                    switchValue));
          }
        }
      } else {
        for (SegmentMetadata entry : volumeMetadata.getSegments()) {
          if (entry.getSegmentUnitCount() > 0) {
            // if the number of segment unit in segment is greater than zero, add the segment
            segmentsMetadataThrift
                .add(buildThriftSegmentMetadataFromSwitch(entry, false, checkSet, switchValue));
          }
        }
      }

      if (segmentsMetadataThrift.size() != volumeMetadata.getSegments().size()) {
        logger.warn("can not build enough segment thrift:{}, from volume:{}",
            segmentsMetadataThrift.size(),
            volumeMetadata.getSegments().size());
      }
    }

    volumeMetadataThrift.setSegmentsMetadataSwitch(segmentsMetadataThrift);
    volumeMetadataThrift.setFreeSpaceRatio(volumeMetadata.getFreeSpaceRatio());
    if (volumeMetadata.getVolumeCreatedTime() != null) {
      volumeMetadataThrift
          .setVolumeCreatedTime(volumeMetadata.getVolumeCreatedTime().getTime());
    }
    if (volumeMetadata.getLastExtendedTime() != null) {
      volumeMetadataThrift.setLastExtendedTime(volumeMetadata.getLastExtendedTime().getTime());
    }
    if (buildThriftVolumeSource(volumeMetadata.getVolumeSource()) != null) {
      volumeMetadataThrift
          .setVolumeSource(buildThriftVolumeSource(volumeMetadata.getVolumeSource()));
    }
    volumeMetadataThrift.setReadWrite(buildThriftReadWriteType(volumeMetadata.getReadWrite()));
    volumeMetadataThrift.setInAction(buildThriftVolumeInAction(volumeMetadata.getInAction()));
    volumeMetadataThrift.setPageWrappCount(volumeMetadata.getPageWrappCount());
    volumeMetadataThrift.setSegmentWrappCount(volumeMetadata.getSegmentWrappCount());
    volumeMetadataThrift.setEnableLaunchMultiDrivers(volumeMetadata.isEnableLaunchMultiDrivers());

    volumeMetadataThrift.setMigrationSpeed(volumeMetadata.getMigrationSpeed());
    volumeMetadataThrift.setMigrationRatio(volumeMetadata.getMigrationRatio());
    volumeMetadataThrift.setTotalPageToMigrate(volumeMetadata.getTotalPageToMigrate());
    volumeMetadataThrift.setAlreadyMigratedPage(volumeMetadata.getAlreadyMigratedPage());

    long volumeTotalMigrationPage = volumeMetadata.getTotalPageToMigrate();
    long volumeAlreadyMigrationPage = volumeMetadata.getAlreadyMigratedPage();
    volumeMetadataThrift.setMigrationRatio(
        0 == volumeTotalMigrationPage ? 100
            : (volumeAlreadyMigrationPage * 100) / volumeTotalMigrationPage);

    volumeMetadataThrift.setRebalanceRatio(volumeMetadata.getRebalanceInfo().getRebalanceRatio());
    volumeMetadataThrift
        .setRebalanceVersion(volumeMetadata.getRebalanceInfo().getRebalanceVersion());
    volumeMetadataThrift.setStableTime(volumeMetadata.getStableTime());

    volumeMetadataThrift.setSwitchStructValue(switchValue);
    volumeMetadataThrift.setTotalPhysicalSpace(volumeMetadata.getTotalPhysicalSpace());
    volumeMetadataThrift.setEachTimeExtendVolumeSize(volumeMetadata.getEachTimeExtendVolumeSize());
    volumeMetadataThrift.setMarkDelete(volumeMetadata.isMarkDelete());
    volumeMetadataThrift.setVolumeDescription(volumeMetadata.getVolumeDescription());
    volumeMetadataThrift.setClientLastConnectTime(volumeMetadata.getClientLastConnectTime());

    logger.debug("after buildThriftVolumeFrom Switch volumeMetadata info is {}",
        volumeMetadataThrift);
    return volumeMetadataThrift;
  }

  public static List<SegmentMetadataThrift> buildThriftSegmentListFrom(
      VolumeMetadata volumeMetadata,
      int startSegmentIndex, int endSegmentIndex) {
    List<SegmentMetadataThrift> segmentsMetadataThrift = new ArrayList<>();

    for (SegmentMetadata entry : volumeMetadata.getSegments()) {
      if (entry.getSegmentUnitCount() > 0 && entry.getIndex() >= startSegmentIndex
          && entry.getIndex() <= endSegmentIndex) {
        // if the number of segment unit in segment is greater than zero and in the range, add
        // the segment
        segmentsMetadataThrift.add(buildThriftSegmentMetadataFrom(entry, false));
      }
    }
    return segmentsMetadataThrift;
  }

  public static VolumeDeleteDelayInformationThrift buildThriftVolumeDeleteDelayFrom(
      VolumeDeleteDelayInformation volumeDeleteDelayInformation) {
    VolumeDeleteDelayInformationThrift volumeDeleteDelayInformationThrift =
        new VolumeDeleteDelayInformationThrift();
    volumeDeleteDelayInformationThrift
        .setTimeForDelay(volumeDeleteDelayInformation.getTimeForDelay());
    volumeDeleteDelayInformationThrift.setStopDelay(volumeDeleteDelayInformation.isStopDelay());
    volumeDeleteDelayInformationThrift.setVolumeId(volumeDeleteDelayInformation.getVolumeId());
    return volumeDeleteDelayInformationThrift;
  }

  public static VolumeRecycleInformationThrift buildThriftVolumeRecycleInformationFrom(
      VolumeRecycleInformation volumeRecycleInformation) {
    VolumeRecycleInformationThrift volumeRecycleInformationThrift =
        new VolumeRecycleInformationThrift();
    volumeRecycleInformationThrift.setTimeInRecycle(volumeRecycleInformation.getTimeInRecycle());
    volumeRecycleInformationThrift.setVolumeId(volumeRecycleInformation.getVolumeId());
    return volumeRecycleInformationThrift;
  }

  public static List<VolumeMetadataThrift> buildThriftVolumesFrom(
      List<VolumeMetadata> volumesMetadata) {
    List<VolumeMetadataThrift> volumesMetadataThrift = new ArrayList<VolumeMetadataThrift>();

    for (VolumeMetadata volume : volumesMetadata) {
      volumesMetadataThrift.add(buildThriftVolumeFrom(volume, true));
    }
    return volumesMetadataThrift;
  }

  public static VolumeMetadata buildVolumeFrom(VolumeMetadataThrift volumeMetadataThrift) {
    //find the switch value and Reversal it
    Map<Short, Long> switchValueReversal = new HashMap<>();
    if (!volumeMetadataThrift.getSwitchStructValue().isEmpty()) {
      for (Map.Entry<Long, Short> entry : volumeMetadataThrift.getSwitchStructValue().entrySet()) {
        switchValueReversal.put(entry.getValue(), entry.getKey());
      }
    }

    VolumeMetadata volumeMetadata = new VolumeMetadata();
    volumeMetadata.setVolumeId(volumeMetadataThrift.getVolumeId());
    volumeMetadata.setName(volumeMetadataThrift.getName());
    volumeMetadata.setVolumeSize(volumeMetadataThrift.getVolumeSize());
    volumeMetadata.setSegmentSize(volumeMetadataThrift.getSegmentSize());
    volumeMetadata.setVolumeType(convertVolumeType(volumeMetadataThrift.getVolumeType()));
    volumeMetadata.setVolumeStatus(convertVolumeStatus(volumeMetadataThrift.getVolumeStatus()));
    volumeMetadata.setAccountId(volumeMetadataThrift.getAccountId());
    volumeMetadata.setRootVolumeId(volumeMetadataThrift.getRootVolumeId());
    volumeMetadata.setExtendingSize(volumeMetadataThrift.getExtendingSize());
    volumeMetadata.setDeadTime(volumeMetadataThrift.getDeadTime());
    volumeMetadata
        .setSegmentNumToCreateEachTime(volumeMetadataThrift.getLeastSegmentUnitAtBeginning());
    volumeMetadata
        .setSegmentNumToCreateEachTime(volumeMetadataThrift.getSegmentNumToCreateEachTime());
    volumeMetadata.setVolumeLayout(volumeMetadataThrift.getVolumeLayout());
    volumeMetadata.setEnableLaunchMultiDrivers(volumeMetadataThrift.isEnableLaunchMultiDrivers());
    volumeMetadata.setTotalPageToMigrate(volumeMetadataThrift.getTotalPageToMigrate());
    volumeMetadata.setAlreadyMigratedPage(volumeMetadataThrift.getAlreadyMigratedPage());
    volumeMetadata.setMigrationSpeed(volumeMetadataThrift.getMigrationSpeed());
    volumeMetadata.setMigrationRatio(volumeMetadataThrift.getMigrationRatio());

    if (volumeMetadataThrift.isSetDomainId()) {
      volumeMetadata.setDomainId(volumeMetadataThrift.getDomainId());
    }

    if (volumeMetadataThrift.isSetStoragePoolId()) {
      volumeMetadata.setStoragePoolId(volumeMetadataThrift.getStoragePoolId());
    }

    volumeMetadata.setVolumeCreatedTime(new Date(volumeMetadataThrift.getVolumeCreatedTime()));
    if (volumeMetadataThrift.getLastExtendedTime() == 0) {
      volumeMetadata.setLastExtendedTime(null);
    } else {
      volumeMetadata.setLastExtendedTime(new Date(volumeMetadataThrift.getLastExtendedTime()));
    }
    volumeMetadata
        .setVolumeSource(buildVolumeSourceTypeFrom(volumeMetadataThrift.getVolumeSource()));
    volumeMetadata.setReadWrite(buildReadWriteTypeFrom(volumeMetadataThrift.getReadWrite()));
    volumeMetadata.setInAction(buildVolumeInActionFrom(volumeMetadataThrift.getInAction()));

    volumeMetadata.setPageWrappCount(volumeMetadataThrift.getPageWrappCount());
    volumeMetadata.setSegmentWrappCount(volumeMetadataThrift.getSegmentWrappCount());

    /* the normal, get segment meta data from the volume ***/
    // if (switchValueReversal.isEmpty()) {
    if (false) {
      logger.info("get in the un switch" + volumeMetadataThrift);
      List<SegmentMetadataThrift> segmentMetadataThrifts = volumeMetadataThrift
          .getSegmentsMetadata();
      for (SegmentMetadataThrift segmentMetadataThrift : segmentMetadataThrifts) {
        // SegId segId = new SegId(volumeMetadata.getVolumeId(), segmentMetadataThrift
        // .getSegIndex());

        SegmentMetadata segmentMetadata = new SegmentMetadata(
            new SegId(segmentMetadataThrift.getVolumeId(), segmentMetadataThrift.getSegId()),
            segmentMetadataThrift.getIndexInVolume());
        SegmentMembership highestMembershipInSegment = null;

        segmentMetadata.setFreeRatio(segmentMetadataThrift.getFreeSpaceRatio());
        for (SegmentUnitMetadataThrift segUnitMetaThrift : segmentMetadataThrift
            .getSegmentUnits()) {
          SegmentUnitMetadata segmentUnit = RequestResponseHelper
              .buildSegmentUnitMetadataFrom(segUnitMetaThrift);
          segmentMetadata
              .putSegmentUnitMetadata(new InstanceId(segUnitMetaThrift.getInstanceId()),
                  segmentUnit);
          SegmentMembership currentMembership = segmentUnit.getMembership();
          if (null == highestMembershipInSegment) {
            highestMembershipInSegment = currentMembership;
          } else if (null != currentMembership
              && currentMembership.compareVersion(highestMembershipInSegment) > 0) {
            highestMembershipInSegment = currentMembership;
          }
        }

        volumeMetadata.addSegmentMetadata(segmentMetadata, highestMembershipInSegment);
      }
    } else {
      /* the switch, get segment meta data from the volume ***/
      logger.debug("when buildVolumeFrom in switch, get the volumeMetadataThrift:{} ",
          volumeMetadataThrift);
      List<SegmentMetadataSwitchThrift> segmentMetadataSwitchThrifts = volumeMetadataThrift
          .getSegmentsMetadataSwitch();
      for (SegmentMetadataSwitchThrift segmentMetadataSwitchThrift :
          segmentMetadataSwitchThrifts) {
        Short volumeIdSwitch = segmentMetadataSwitchThrift.getVolumeIdSwitch();

        SegmentMetadata segmentMetadata = new SegmentMetadata(
            new SegId(switchValueReversal.get(volumeIdSwitch),
                segmentMetadataSwitchThrift.getSegId()),
            segmentMetadataSwitchThrift.getIndexInVolume());
        SegmentMembership highestMembershipInSegment = null;

        segmentMetadata.setFreeRatio(segmentMetadataSwitchThrift.getFreeSpaceRatio());
        for (SegmentUnitMetadataSwitchThrift segmentUnitMetadataSwitchThrift :
            segmentMetadataSwitchThrift
                .getSegmentUnits()) {
          SegmentUnitMetadata segmentUnit = RequestResponseHelper
              .buildSegmentUnitMetadataFromSwitch(segmentUnitMetadataSwitchThrift,
                  switchValueReversal);
          Short id = segmentUnitMetadataSwitchThrift.getInstanceIdSwitch();
          long instanceId = switchValueReversal.get(id);
          segmentMetadata.putSegmentUnitMetadata(new InstanceId(instanceId), segmentUnit);

          SegmentMembership currentMembership = segmentUnit.getMembership();
          if (null == highestMembershipInSegment) {
            highestMembershipInSegment = currentMembership;
          } else if (null != currentMembership
              && currentMembership.compareVersion(highestMembershipInSegment) > 0) {
            highestMembershipInSegment = currentMembership;
          }
        }
        volumeMetadata.addSegmentMetadata(segmentMetadata, highestMembershipInSegment);
        logger.debug("after buildVolumeFrom switch, the volumeMetadata:{}", volumeMetadata);

      }
    }

    volumeMetadata.setFreeSpaceRatio(volumeMetadataThrift.getFreeSpaceRatio());
    volumeMetadata.getRebalanceInfo().setRebalanceRatio(volumeMetadataThrift.getRebalanceRatio());
    volumeMetadata.getRebalanceInfo()
        .setRebalanceVersion(volumeMetadataThrift.getRebalanceVersion());
    volumeMetadata.setStableTime(volumeMetadataThrift.getStableTime());
    volumeMetadata.setTotalPhysicalSpace(volumeMetadataThrift.getTotalPhysicalSpace());
    volumeMetadata.setEachTimeExtendVolumeSize(volumeMetadataThrift.getEachTimeExtendVolumeSize());
    volumeMetadata.setMarkDelete(volumeMetadataThrift.isMarkDelete());
    volumeMetadata.setClientLastConnectTime(volumeMetadataThrift.getClientLastConnectTime());
    volumeMetadata.setVolumeDescription(volumeMetadataThrift.getVolumeDescription());

    return volumeMetadata;
  }

  public static VolumeDeleteDelayInformation buildVolumeDeleteDelayFrom(
      VolumeDeleteDelayInformationThrift volumeDeleteDelayInformationThrift) {
    VolumeDeleteDelayInformation volumeDeleteDelayInformation = new VolumeDeleteDelayInformation();
    volumeDeleteDelayInformation
        .setTimeForDelay(volumeDeleteDelayInformationThrift.getTimeForDelay());
    volumeDeleteDelayInformation.setStopDelay(volumeDeleteDelayInformationThrift.isStopDelay());
    volumeDeleteDelayInformation.setVolumeId(volumeDeleteDelayInformationThrift.getVolumeId());
    return volumeDeleteDelayInformation;
  }

  public static VolumeRecycleInformation buildVolumeRecycleInformationFrom(
      VolumeRecycleInformationThrift volumeRecycleInformationThrift) {
    VolumeRecycleInformation volumeRecycleInformation = new VolumeRecycleInformation();
    volumeRecycleInformation.setTimeInRecycle(volumeRecycleInformationThrift.getTimeInRecycle());
    volumeRecycleInformation.setVolumeId(volumeRecycleInformationThrift.getVolumeId());
    return volumeRecycleInformation;
  }

  @Deprecated
  public static VolumeMetadata buildVolumeFrom_back(VolumeMetadataThrift volumeMetadataThrift) {
    VolumeMetadata volumeMetadata = new VolumeMetadata();
    volumeMetadata.setVolumeId(volumeMetadataThrift.getVolumeId());
    volumeMetadata.setName(volumeMetadataThrift.getName());
    volumeMetadata.setVolumeSize(volumeMetadataThrift.getVolumeSize());
    volumeMetadata.setSegmentSize(volumeMetadataThrift.getSegmentSize());
    volumeMetadata.setVolumeType(convertVolumeType(volumeMetadataThrift.getVolumeType()));
    volumeMetadata.setVolumeStatus(convertVolumeStatus(volumeMetadataThrift.getVolumeStatus()));
    volumeMetadata.setAccountId(volumeMetadataThrift.getAccountId());
    volumeMetadata.setRootVolumeId(volumeMetadataThrift.getRootVolumeId());
    volumeMetadata.setExtendingSize(volumeMetadataThrift.getExtendingSize());
    volumeMetadata.setDeadTime(volumeMetadataThrift.getDeadTime());
    volumeMetadata
        .setSegmentNumToCreateEachTime(volumeMetadataThrift.getLeastSegmentUnitAtBeginning());
    volumeMetadata
        .setSegmentNumToCreateEachTime(volumeMetadataThrift.getSegmentNumToCreateEachTime());
    volumeMetadata.setVolumeLayout(volumeMetadataThrift.getVolumeLayout());
    volumeMetadata.setEnableLaunchMultiDrivers(volumeMetadataThrift.isEnableLaunchMultiDrivers());

    //        try {
    //            volumeMetadata.setSnapshotManager(DistributedVolumeSnapshotManagerImpl
    //                    .parseFromByteArray(volumeMetadataThrift.getSnapshotManagerInBinary(),
    //                            volumeMetadataThrift.getVolumeId()));
    //        } catch (IOException e) {
    //            logger.error("can not parse ", volumeMetadataThrift);
    //        }

    if (volumeMetadataThrift.isSetDomainId()) {
      volumeMetadata.setDomainId(volumeMetadataThrift.getDomainId());
    }

    if (volumeMetadataThrift.isSetStoragePoolId()) {
      volumeMetadata.setStoragePoolId(volumeMetadataThrift.getStoragePoolId());
    }

    volumeMetadata.setVolumeCreatedTime(new Date(volumeMetadataThrift.getVolumeCreatedTime()));
    if (volumeMetadataThrift.getLastExtendedTime() == 0) {
      volumeMetadata.setLastExtendedTime(null);
    } else {
      volumeMetadata.setLastExtendedTime(new Date(volumeMetadataThrift.getLastExtendedTime()));
    }
    volumeMetadata
        .setVolumeSource(buildVolumeSourceTypeFrom(volumeMetadataThrift.getVolumeSource()));
    volumeMetadata.setReadWrite(buildReadWriteTypeFrom(volumeMetadataThrift.getReadWrite()));
    volumeMetadata.setInAction(buildVolumeInActionFrom(volumeMetadataThrift.getInAction()));

    volumeMetadata.setPageWrappCount(volumeMetadataThrift.getPageWrappCount());
    volumeMetadata.setSegmentWrappCount(volumeMetadataThrift.getSegmentWrappCount());

    // get segment meta data from the volume
    List<SegmentMetadataThrift> segmentMetadataThrifts = volumeMetadataThrift.getSegmentsMetadata();
    for (SegmentMetadataThrift segmentMetadataThrift : segmentMetadataThrifts) {
      // SegId segId = new SegId(volumeMetadata.getVolumeId(), segmentMetadataThrift.getSegIndex());

      SegmentMetadata segmentMetadata = new SegmentMetadata(
          new SegId(segmentMetadataThrift.getVolumeId(), segmentMetadataThrift.getSegId()),
          segmentMetadataThrift.getIndexInVolume());
      SegmentMembership highestMembershipInSegment = null;
      segmentMetadata.setFreeRatio(segmentMetadataThrift.getFreeSpaceRatio());
      for (SegmentUnitMetadataThrift segUnitMetaThrift : segmentMetadataThrift.getSegmentUnits()) {
        SegmentUnitMetadata segmentUnit = RequestResponseHelper
            .buildSegmentUnitMetadataFrom(segUnitMetaThrift);
        segmentMetadata
            .putSegmentUnitMetadata(new InstanceId(segUnitMetaThrift.getInstanceId()), segmentUnit);
        SegmentMembership currentMembership = segmentUnit.getMembership();
        if (null == highestMembershipInSegment) {
          highestMembershipInSegment = currentMembership;
        } else if (null != currentMembership
            && currentMembership.compareVersion(highestMembershipInSegment) > 0) {
          highestMembershipInSegment = currentMembership;
        }
      }

      volumeMetadata.addSegmentMetadata(segmentMetadata, highestMembershipInSegment);
    }
    volumeMetadata.setFreeSpaceRatio(volumeMetadataThrift.getFreeSpaceRatio());

    volumeMetadata.getRebalanceInfo().setRebalanceRatio(volumeMetadataThrift.getRebalanceRatio());
    volumeMetadata.getRebalanceInfo()
        .setRebalanceVersion(volumeMetadataThrift.getRebalanceVersion());
    volumeMetadata.setStableTime(volumeMetadataThrift.getStableTime());
    volumeMetadata.setMarkDelete(volumeMetadataThrift.isMarkDelete());
    return volumeMetadata;
  }

  @Deprecated
  public static List<VolumeMetadata> buildVolumesFrom(
      List<VolumeMetadataThrift> volumeMetadataThrifts) {
    List<VolumeMetadata> volumes = new ArrayList<>(volumeMetadataThrifts.size());

    for (VolumeMetadataThrift volumeThrift : volumeMetadataThrifts) {
      volumes.add(buildVolumeFrom(volumeThrift));
    }

    return volumes;
  }

  public static InstanceMetadata.DatanodeStatus convertFromDatanodeStatusThrift(
      DatanodeStatusThrift datanodeStatusThrift) {
    InstanceMetadata.DatanodeStatus datanodeStatus = null;
    if (datanodeStatusThrift == DatanodeStatusThrift.OK) {
      datanodeStatus = InstanceMetadata.DatanodeStatus.OK;
    } else if (datanodeStatusThrift == DatanodeStatusThrift.UNKNOWN) {
      datanodeStatus = InstanceMetadata.DatanodeStatus.UNKNOWN;
    } else if (datanodeStatusThrift != null) {
      Validate.isTrue(false, "unknown datanode status: " + datanodeStatusThrift);
    }
    return datanodeStatus;
  }

  public static DatanodeStatusThrift convertThriftDatanodeStatus(
      InstanceMetadata.DatanodeStatus datanodeStatus) {
    DatanodeStatusThrift datanodeStatusThrift = null;
    if (datanodeStatus == InstanceMetadata.DatanodeStatus.OK) {
      datanodeStatusThrift = DatanodeStatusThrift.OK;
    } else if (datanodeStatus == InstanceMetadata.DatanodeStatus.UNKNOWN) {
      datanodeStatusThrift = DatanodeStatusThrift.UNKNOWN;
    } else if (datanodeStatus != null) {
      Validate.isTrue(false, "unknown datanode status: " + datanodeStatus);
    }
    return datanodeStatusThrift;
  }

  public static InstanceMetadataThrift buildThriftInstanceFromAfterFilter(InstanceMetadata instance)
      throws DatanodeTypeNotSetExceptionThrift {
    Validate.notNull(instance);
    InstanceMetadataThrift instanceMetadataThrift = new InstanceMetadataThrift();
    instanceMetadataThrift.setInstanceId(instance.getInstanceId().getId());
    instanceMetadataThrift.setEndpoint(instance.getEndpoint());
    instanceMetadataThrift.setFreeSpace(instance.getFreeSpace());
    instanceMetadataThrift.setLogicalCapacity(instance.getLogicalCapacity());
    instanceMetadataThrift.setCapacity(instance.getCapacity());
    List<ArchiveMetadataThrift> archivesThrift = new ArrayList<>();
    Set<Long> archiveIdSet = new HashSet<>();
    for (ArchiveMetadata archiveMetadata : instance.getArchiveMetadatas()) {
      archivesThrift.add(buildThriftArchiveMetadataFrom(archiveMetadata));
      archiveIdSet.add(archiveMetadata.getArchiveId());
    }
    for (RawArchiveMetadata archiveMetadata : instance.getArchives()) {
      if (archiveIdSet.contains(archiveMetadata.getArchiveId())) {
        continue;
      }
      archivesThrift.add(buildThriftArchiveMetadataFrom(archiveMetadata));
    }

    instanceMetadataThrift.setArchiveMetadata(archivesThrift);
    instanceMetadataThrift.setGroup(buildThriftGroupFrom(instance.getGroup()));
    instanceMetadataThrift
        .setInstanceDomain(buildThriftInstanceDomainFrom(instance.getInstanceDomain()));
    instanceMetadataThrift
        .setDatanodeStatus(convertThriftDatanodeStatus(instance.getDatanodeStatus()));
    instanceMetadataThrift
        .setDatanodeType(convertDatanodeType2DatanodeTypeThrift(instance.getDatanodeType()));

    return instanceMetadataThrift;
  }

  public static InstanceMetadataThrift buildThriftInstanceFrom(InstanceMetadata instance)
      throws DatanodeTypeNotSetExceptionThrift {
    Validate.notNull(instance);
    InstanceMetadataThrift instanceMetadataThrift = new InstanceMetadataThrift();
    instanceMetadataThrift.setInstanceId(instance.getInstanceId().getId());
    instanceMetadataThrift.setEndpoint(instance.getEndpoint());
    instanceMetadataThrift.setFreeSpace(instance.getFreeSpace());
    instanceMetadataThrift.setLogicalCapacity(instance.getLogicalCapacity());
    instanceMetadataThrift.setCapacity(instance.getCapacity());
    List<ArchiveMetadataThrift> archivesThrift = new ArrayList<>();
    for (RawArchiveMetadata archiveMetadata : instance.getArchives()) {
      archivesThrift.add(buildThriftArchiveMetadataFrom(archiveMetadata));
    }
    for (ArchiveMetadata archiveMetadata : instance.getArchiveMetadatas()) {
      archivesThrift.add(buildThriftArchiveMetadataFrom(archiveMetadata));
    }

    instanceMetadataThrift.setArchiveMetadata(archivesThrift);
    instanceMetadataThrift.setGroup(buildThriftGroupFrom(instance.getGroup()));
    instanceMetadataThrift
        .setInstanceDomain(buildThriftInstanceDomainFrom(instance.getInstanceDomain()));
    instanceMetadataThrift
        .setDatanodeStatus(convertThriftDatanodeStatus(instance.getDatanodeStatus()));
    instanceMetadataThrift
        .setDatanodeType(convertDatanodeType2DatanodeTypeThrift(instance.getDatanodeType()));

    return instanceMetadataThrift;
  }

  public static InstanceMetadataThrift buildThriftInstanceFromSeparatedDatanode(
      InstanceMetadata instance)
      throws DatanodeTypeNotSetExceptionThrift {
    Validate.notNull(instance);
    InstanceMetadataThrift instanceMetadataThrift = new InstanceMetadataThrift();
    instanceMetadataThrift.setInstanceId(instance.getInstanceId().getId());
    instanceMetadataThrift.setEndpoint(instance.getEndpoint());
    instanceMetadataThrift.setFreeSpace(instance.getFreeSpace());
    instanceMetadataThrift.setLogicalCapacity(instance.getLogicalCapacity());
    instanceMetadataThrift.setCapacity(instance.getCapacity());
    List<ArchiveMetadataThrift> archivesThrift = new ArrayList<>();
    for (RawArchiveMetadata archiveMetadata : instance.getArchives()) {
      ArchiveMetadataThrift archiveMetadataThrift = buildThriftArchiveMetadataFrom(
          archiveMetadata);
      archiveMetadataThrift.setStatus(ArchiveStatusThrift.SEPARATED);
      archivesThrift.add(archiveMetadataThrift);
    }
    for (ArchiveMetadata archiveMetadata : instance.getArchiveMetadatas()) {
      ArchiveMetadataThrift archiveMetadataThrift = buildThriftArchiveMetadataFrom(
          archiveMetadata);
      archiveMetadataThrift.setStatus(ArchiveStatusThrift.SEPARATED);
      archivesThrift.add(archiveMetadataThrift);
    }

    instanceMetadataThrift.setArchiveMetadata(archivesThrift);
    instanceMetadataThrift.setGroup(buildThriftGroupFrom(instance.getGroup()));
    instanceMetadataThrift
        .setInstanceDomain(buildThriftInstanceDomainFrom(instance.getInstanceDomain()));
    instanceMetadataThrift.setDatanodeStatus(DatanodeStatusThrift.SEPARATED);
    instanceMetadataThrift
        .setDatanodeType(convertDatanodeType2DatanodeTypeThrift(instance.getDatanodeType()));

    return instanceMetadataThrift;
  }

  public static InstanceMetadataThrift buildThriftInstanceFromForArchiveId(
      InstanceMetadata instance, Long archiveId)
      throws DatanodeTypeNotSetExceptionThrift {
    Validate.notNull(instance);
    InstanceMetadataThrift instanceMetadataThrift = new InstanceMetadataThrift();
    instanceMetadataThrift.setInstanceId(instance.getInstanceId().getId());
    instanceMetadataThrift.setEndpoint(instance.getEndpoint());
    instanceMetadataThrift.setFreeSpace(instance.getFreeSpace());
    instanceMetadataThrift.setLogicalCapacity(instance.getLogicalCapacity());
    instanceMetadataThrift.setCapacity(instance.getCapacity());
    List<ArchiveMetadataThrift> archivesThrift = new ArrayList<>();
    for (RawArchiveMetadata archiveMetadata : instance.getArchives()) {
      if (archiveMetadata != null && archiveMetadata.getArchiveId().equals(archiveId)) {
        archivesThrift.add(buildThriftArchiveMetadataFrom(archiveMetadata));
      }
    }
    for (ArchiveMetadata archiveMetadata : instance.getArchiveMetadatas()) {
      if (archiveMetadata != null && archiveMetadata.getArchiveId().equals(archiveId)) {
        archivesThrift.add(buildThriftArchiveMetadataFrom(archiveMetadata));
      }
    }

    instanceMetadataThrift.setArchiveMetadata(archivesThrift);
    instanceMetadataThrift.setGroup(buildThriftGroupFrom(instance.getGroup()));
    instanceMetadataThrift
        .setInstanceDomain(buildThriftInstanceDomainFrom(instance.getInstanceDomain()));
    instanceMetadataThrift
        .setDatanodeStatus(convertThriftDatanodeStatus(instance.getDatanodeStatus()));
    instanceMetadataThrift
        .setDatanodeType(convertDatanodeType2DatanodeTypeThrift(instance.getDatanodeType()));

    return instanceMetadataThrift;
  }

  public static InstanceIdAndEndPointThrift buildInstanceIdAndEndPointFrom(
      InstanceMetadata instance) {
    Validate.notNull(instance);
    InstanceIdAndEndPointThrift instanceIdAndEndPointThrift = new InstanceIdAndEndPointThrift();
    instanceIdAndEndPointThrift.setInstanceId(instance.getInstanceId().getId());
    instanceIdAndEndPointThrift.setEndPoint(instance.getEndpoint());
    instanceIdAndEndPointThrift.setGroupId(instance.getGroup().getGroupId());
    return instanceIdAndEndPointThrift;
  }

  public static InstanceMetadata buildInstanceFrom(InstanceMetadataThrift instanceMetadataThrift)
      throws DatanodeTypeNotSetExceptionThrift {
    logger
        .debug("buildInstanceFrom begin,the InstanceMetadataThrift is {}", instanceMetadataThrift);
    Validate.notNull(instanceMetadataThrift);
    InstanceMetadata instanceMetadata = new InstanceMetadata(
        new InstanceId(instanceMetadataThrift.getInstanceId()));
    instanceMetadata.setEndpoint(instanceMetadataThrift.getEndpoint());
    instanceMetadata.setCapacity(instanceMetadataThrift.getCapacity());
    instanceMetadata.setFreeSpace(instanceMetadataThrift.getFreeSpace());
    instanceMetadata.setLogicalCapacity(instanceMetadataThrift.getLogicalCapacity());
    List<RawArchiveMetadata> archives = new ArrayList<>();
    List<ArchiveMetadata> archiveMetadatas = new ArrayList<>();
    for (ArchiveMetadataThrift archiveMetadataThrift : instanceMetadataThrift
        .getArchiveMetadata()) {
      if (archiveMetadataThrift.getType() == ArchiveTypeThrift.RAW_DISK) {
        RawArchiveMetadata archive = buildArchiveMetadataFrom(archiveMetadataThrift);
        archive.setInstanceId(new InstanceId(instanceMetadataThrift.getInstanceId()));
        archives.add(archive);
      } else {
        ArchiveMetadata archiveMetadata = buildOtherArchiveMetadataFrom(archiveMetadataThrift);
        archiveMetadata.setInstanceId(new InstanceId(instanceMetadataThrift.getInstanceId()));
        archiveMetadatas.add(archiveMetadata);
      }
    }

    instanceMetadata.setArchives(archives);
    instanceMetadata.setArchiveMetadatas(archiveMetadatas);
    instanceMetadata.setGroup(buildGroupFrom(instanceMetadataThrift.getGroup()));
    instanceMetadata
        .setInstanceDomain(buildInstanceDomainFrom(instanceMetadataThrift.getInstanceDomain()));
    if (instanceMetadataThrift.isSetDatanodeStatus()) {
      instanceMetadata
          .setDatanodeStatus(
              convertFromDatanodeStatusThrift(instanceMetadataThrift.getDatanodeStatus()));
    } else {
      instanceMetadata.setDatanodeStatus(InstanceMetadata.DatanodeStatus.OK);
    }

    //Datanode Type
    instanceMetadata
        .setDatanodeType(
            convertDatanodeTypeThrift2DatanodeType(instanceMetadataThrift.getDatanodeType()));

    logger.debug("buildInstanceFrom end,the InstanceMetadata to be return is {}", instanceMetadata);
    return instanceMetadata;
  }

  public static VolumeCreationRequest buildCreateVolumeRequest(long segmentSize,
      py.thrift.icshare.CreateVolumeRequest request) {
    CacheType cacheType = null;
    VolumeCreationRequest volumeRequest = new VolumeCreationRequest(request.getVolumeId(),
        request.getVolumeSize(),
        convertVolumeType(request.getVolumeType()), request.getAccountId(),
        request.isEnableLaunchMultiDrivers());
    volumeRequest.setName(request.getName());
    volumeRequest.setDomainId(request.getDomainId());
    volumeRequest.setStoragePoolId(request.getStoragePoolId());
    volumeRequest.setSegmentSize(segmentSize);
    volumeRequest.setRequestType(RequestType.CREATE_VOLUME);

    volumeRequest.setVolumeDescription(request.getVolumeDescription());

    return volumeRequest;
  }

  // this is used when creating segment for a non-complete volume, so we only need volumeId,
  // segmentSize and
  // volumeType

  public static VolumeCreationRequest buildCreateVolumeRequestFrom(long segmentSize,
      py.thrift.infocenter.service.CreateSegmentsRequest request) {
    VolumeCreationRequest volumeRequest = new VolumeCreationRequest(request.getVolumeId(),
        request.getNumToCreate() * segmentSize, convertVolumeType(request.getVolumeType()),
        request.getAccountId(), false);
    volumeRequest.setSegmentSize(segmentSize);
    volumeRequest.setRequestType(RequestType.CREATE_VOLUME);
    volumeRequest.setDomainId(request.getDomainId());
    volumeRequest.setStoragePoolId(request.getStoragePoolId());
    return volumeRequest;
  }

  public static VolumeCreationRequest buildExtendVolumeRequest(long segmentSize,
      VolumeMetadata volumeToExtend,
      long extendingSize) {
    VolumeCreationRequest volumeRequest = new VolumeCreationRequest(volumeToExtend.getVolumeId(),
        volumeToExtend.getVolumeId(), extendingSize, volumeToExtend.getVolumeType(),
        volumeToExtend.getAccountId(),
        volumeToExtend.isEnableLaunchMultiDrivers());
    volumeRequest.setName(volumeToExtend.getName());
    volumeRequest.setSegmentSize(segmentSize);
    volumeRequest.setRequestType(RequestType.EXTEND_VOLUME);
    Validate.notNull(volumeToExtend.getDomainId());
    Validate.notNull(volumeToExtend.getStoragePoolId());
    volumeRequest.setDomainId(volumeToExtend.getDomainId());
    volumeRequest.setStoragePoolId(volumeToExtend.getStoragePoolId());
    return volumeRequest;
  }

  public static InstanceMetadata buildInstanceWithPartOfArchives(InstanceMetadata instanceMetadata,
      Collection<Long> archiveIds) {
    logger.debug("buildInstanceFrom begin,the instanceMetadata is {}, and archiveIds:{}",
        instanceMetadata,
        archiveIds);
    Validate.notNull(instanceMetadata);
    InstanceMetadata buildInstanceMetadata = new InstanceMetadata(
        new InstanceId(instanceMetadata.getInstanceId()));
    buildInstanceMetadata.setEndpoint(instanceMetadata.getEndpoint());
    buildInstanceMetadata.setCapacity(instanceMetadata.getCapacity());
    buildInstanceMetadata.setLogicalCapacity(instanceMetadata.getLogicalCapacity());

    Long freeSpace = 0L;
    List<RawArchiveMetadata> archives = new ArrayList<>();
    for (Long archiveId : archiveIds) {
      RawArchiveMetadata archiveMetadata = instanceMetadata.getArchiveById(archiveId);
      if (archiveMetadata != null) {
        archives.add(archiveMetadata);
        freeSpace += archiveMetadata.getLogicalFreeSpace();
      }
    }

    buildInstanceMetadata.setFreeSpace(freeSpace);
    buildInstanceMetadata.setArchives(archives);
    buildInstanceMetadata.setGroup(instanceMetadata.getGroup());
    buildInstanceMetadata.setInstanceDomain(instanceMetadata.getInstanceDomain());
    buildInstanceMetadata.setDatanodeType(instanceMetadata.getDatanodeType());
    logger.debug("buildInstanceWithPartOfArchives end,the InstanceMetadata to be return is {}",
        buildInstanceMetadata);
    return buildInstanceMetadata;
  }

  public static AccountMetadataThrift buildAccountMetadataThriftFrom(
      AccountMetadata accountMetadata) {
    AccountMetadataThrift accountThrift = new AccountMetadataThrift();
    accountThrift.setAccountId(accountMetadata.getAccountId());
    accountThrift.setAccountName(accountMetadata.getAccountName());
    if (null != accountMetadata.getAccountType()) {
      accountThrift.setAccountType(AccountTypeThrift.valueOf(accountMetadata.getAccountType()));
    }
    Set<RoleThrift> roleThrifts = new HashSet<>();
    accountThrift.setRoles(roleThrifts);
    for (Role role : accountMetadata.getRoles()) {
      roleThrifts.add(buildRoleThrift(role));
    }
    Set<ResourceThrift> resourceThrifts = new HashSet<>();
    accountThrift.setResources(resourceThrifts);
    for (PyResource resource : accountMetadata.getResources()) {
      resourceThrifts.add(RequestResponseHelper.buildResourceThrift(resource));
    }
    return accountThrift;
  }

  public static AccountMetadata buildAccountMetadataFrom(AccountMetadataThrift accountThrift) {
    AccountMetadata account = new AccountMetadata();
    account.setAccountId(accountThrift.getAccountId());
    account.setAccountName(accountThrift.getAccountName());
    account.setAccountType(accountThrift.getAccountType().name());
    Set<Role> roles = new HashSet<>();
    account.setRoles(roles);
    for (RoleThrift roleThrift : accountThrift.getRoles()) {
      roles.add(buildRoleFrom(roleThrift));
    }
    Set<PyResource> resources = new HashSet<>();
    account.setResources(resources);
    for (ResourceThrift resourceThrift : accountThrift.getResources()) {
      resources.add(buildResourceFrom(resourceThrift));
    }
    return account;
  }

  public static AccountMetadataBackupThrift buildAccountMetadataBackupThrift(
      AccountMetadata accountMetadata) {
    AccountMetadataBackupThrift accountMetadataBackupThrift = new AccountMetadataBackupThrift();
    accountMetadataBackupThrift.setAccountId(accountMetadata.getAccountId());
    accountMetadataBackupThrift.setAccountName(accountMetadata.getAccountName());
    accountMetadataBackupThrift.setHashedPassword(accountMetadata.getHashedPassword());
    accountMetadataBackupThrift.setSalt(accountMetadata.getSalt());
    if (null != accountMetadata.getAccountType()) {
      accountMetadataBackupThrift.setAccountType(accountMetadata.getAccountType());
    }
    Set<RoleThrift> roleThrifts = new HashSet<>();
    accountMetadataBackupThrift.setRoles(roleThrifts);
    for (Role role : accountMetadata.getRoles()) {
      roleThrifts.add(buildRoleThrift(role));
    }
    Set<ResourceThrift> resourceThrifts = new HashSet<>();
    accountMetadataBackupThrift.setResources(resourceThrifts);
    for (PyResource resource : accountMetadata.getResources()) {
      resourceThrifts.add(buildResourceThrift(resource));
    }
    accountMetadataBackupThrift.setCreatedAt(accountMetadata.getCreatedAt().getTime());
    return accountMetadataBackupThrift;
  }

  public static AccountMetadata buildAccountMetadataBackupFrom(
      AccountMetadataBackupThrift accountBackupThrift) {
    AccountMetadata accountMetadata = new AccountMetadata();
    accountMetadata.setAccountId(accountBackupThrift.getAccountId());
    accountMetadata.setAccountName(accountBackupThrift.getAccountName());
    accountMetadata.setHashedPassword(accountBackupThrift.getHashedPassword());
    accountMetadata.setSalt(accountBackupThrift.getSalt());
    if (null != accountBackupThrift.getAccountType()) {
      accountMetadata.setAccountType(accountBackupThrift.getAccountType());
    }
    Set<Role> roles = new HashSet<>();
    accountMetadata.setRoles(roles);
    for (RoleThrift role : accountBackupThrift.getRoles()) {
      roles.add(buildRoleFrom(role));
    }
    Set<PyResource> resources = new HashSet<>();
    accountMetadata.setResources(resources);
    for (ResourceThrift resource : accountBackupThrift.getResources()) {
      resources.add(buildResourceFrom(resource));
    }
    accountMetadata.setCreatedAt(new Date(accountBackupThrift.getCreatedAt()));
    return accountMetadata;
  }

  public static BroadcastRequest buildDeleteSegmentUnitBroadcastRequest(SegId segId,
      long myInstanceId,
      SegmentMembership currentMembership) {
    BroadcastRequest request = new BroadcastRequest(RequestIdBuilder.get(), 1,
        segId.getVolumeId().getId(),
        segId.getIndex(), BroadcastTypeThrift.DeleteSegmentUnit,
        RequestResponseHelper.buildThriftMembershipFrom(segId, currentMembership));
    request.setMyself(myInstanceId);
    return request;
  }

  public static IscsiAccessRuleThrift buildIscsiAccessRuleThriftFrom(
      IscsiAccessRule iscsiAccessRule) {
    IscsiAccessRuleThrift iscsiAccessRuleToRemote = new IscsiAccessRuleThrift();
    iscsiAccessRuleToRemote.setRuleId(iscsiAccessRule.getRuleId());
    iscsiAccessRuleToRemote.setRuleNotes(iscsiAccessRule.getRuleNotes());
    iscsiAccessRuleToRemote.setInitiatorName(iscsiAccessRule.getInitiatorName());
    iscsiAccessRuleToRemote.setUser(iscsiAccessRule.getUser());
    iscsiAccessRuleToRemote.setPassed(iscsiAccessRule.getPassed());
    iscsiAccessRuleToRemote.setOutUser(iscsiAccessRule.getOutUser());
    iscsiAccessRuleToRemote.setOutPassed(iscsiAccessRule.getOutPassed());
    iscsiAccessRuleToRemote
        .setPermission(AccessPermissionTypeThrift.valueOf(iscsiAccessRule.getPermission().name()));
    iscsiAccessRuleToRemote
        .setStatus(AccessRuleStatusThrift.valueOf(iscsiAccessRule.getStatus().name()));

    return iscsiAccessRuleToRemote;
  }

  public static IscsiAccessRule buildIscsiAccessRuleFrom(
      IscsiAccessRuleThrift iscsiAccessRuleFromRemote) {
    IscsiAccessRule iscsiAccessRule = new IscsiAccessRule();
    iscsiAccessRule.setRuleId(iscsiAccessRuleFromRemote.getRuleId());
    iscsiAccessRule.setRuleNotes(iscsiAccessRuleFromRemote.getRuleNotes());
    iscsiAccessRule.setInitiatorName(iscsiAccessRuleFromRemote.getInitiatorName());
    iscsiAccessRule.setUser(iscsiAccessRuleFromRemote.getUser());
    iscsiAccessRule.setPassed(iscsiAccessRuleFromRemote.getPassed());
    iscsiAccessRule.setOutUser(iscsiAccessRuleFromRemote.getOutUser());
    iscsiAccessRule.setOutPassed(iscsiAccessRuleFromRemote.getOutPassed());
    iscsiAccessRule.setPermission(
        AccessPermissionType.valueOf(iscsiAccessRuleFromRemote.getPermission().name()));
    if (iscsiAccessRuleFromRemote.isSetStatus()) {
      iscsiAccessRule
          .setStatus(AccessRuleStatus.findByName(iscsiAccessRuleFromRemote.getStatus().name()));
    }
    return iscsiAccessRule;
  }

  public static Iscsi2AccessRuleRelationship buildIscsi2AccessRuleRelationshipFromThrift(
      IscsiRuleRelationshipThrift iscsi2RuleThrift) {
    Iscsi2AccessRuleRelationship iscsi2Rule = new Iscsi2AccessRuleRelationship();
    // DriverKey or Separate?
    iscsi2Rule.setDriverContainerId(iscsi2RuleThrift.getDriverKey().getDriverContainerId());
    iscsi2Rule.setVolumeId(iscsi2RuleThrift.getDriverKey().getVolumeId());
    iscsi2Rule.setSnapshotId(iscsi2RuleThrift.getDriverKey().getSnapshotId());
    iscsi2Rule.setDriverType(iscsi2RuleThrift.getDriverKey().getDriverType().name());
    iscsi2Rule.setRelationshipId(iscsi2RuleThrift.getRelationshipId());
    iscsi2Rule.setRuleId(iscsi2RuleThrift.getRuleId());
    iscsi2Rule.setStatus(
        AccessRuleStatusBindingVolume
            .findByName(iscsi2RuleThrift.getAccessRuleStatusBindingVolume().name()));
    return iscsi2Rule;
  }

  public static IscsiRuleRelationshipThrift buildThriftIscsiRuleRelationship(
      Iscsi2AccessRuleRelationship iscsi2Rule) {
    IscsiRuleRelationshipThrift iscsi2RuleThrift = new IscsiRuleRelationshipThrift();

    iscsi2RuleThrift.setDriverKey(
        new DriverKeyThrift(iscsi2Rule.getDriverContainerId(), iscsi2Rule.getVolumeId(),
            iscsi2Rule.getSnapshotId(), DriverTypeThrift.valueOf(iscsi2Rule.getDriverType())));
    iscsi2RuleThrift.setRelationshipId(iscsi2Rule.getRelationshipId());
    iscsi2RuleThrift.setRuleId(iscsi2Rule.getRuleId());
    iscsi2RuleThrift.setAccessRuleStatusBindingVolume(
        AccessRuleStatusBindingVolumeThrift.findByValue(iscsi2Rule.getStatus().getValue()));

    return iscsi2RuleThrift;
  }

  public static Group buildGroupFrom(GroupThrift groupFromRemote) {
    if (groupFromRemote == null) {
      return null;
    }

    Group group = new Group(groupFromRemote.getGroupId());
    return group;
  }

  public static GroupThrift buildThriftGroupFrom(Group group) {
    if (group == null) {
      return null;
    }

    GroupThrift groupToRemote = new GroupThrift(group.getGroupId());
    return groupToRemote;
  }

  public static InstanceDomain buildInstanceDomainFrom(InstanceDomainThrift instanceDomainThrift) {
    if (instanceDomainThrift == null) {
      return new InstanceDomain();
    }

    InstanceDomain instanceDomain = new InstanceDomain(instanceDomainThrift.getDomianId());
    return instanceDomain;
  }

  public static InstanceDomainThrift buildThriftInstanceDomainFrom(InstanceDomain instanceDomain) {
    if (instanceDomain == null || instanceDomain.getDomainId() == null) {
      return null;
    }

    InstanceDomainThrift instanceDomainThrift = new InstanceDomainThrift(
        instanceDomain.getDomainId());
    return instanceDomainThrift;
  }

  public static DomainThrift buildDomainThriftFrom(Domain domain) {
    DomainThrift domainThrift = new DomainThrift();
    if (domain == null) {
      return domainThrift;
    }
    domainThrift.setDomainId(domain.getDomainId());
    domainThrift.setDomainName(domain.getDomainName());
    domainThrift.setDomainDescription(domain.getDomainDescription());
    domainThrift.setStatus(converThriftStatusFrom(domain.getStatus()));
    domainThrift.setLastUpdateTime(domain.getLastUpdateTime());
    Set<Long> dataNodeList = new HashSet<>();
    if (domain.getDataNodes() != null && !domain.getDataNodes().isEmpty()) {
      dataNodeList.addAll(domain.getDataNodes());
    }
    domainThrift.setDatanodes(dataNodeList);

    Set<Long> storagePoolList = new HashSet<>();
    if (domain.getStoragePools() != null && !domain.getStoragePools().isEmpty()) {
      storagePoolList.addAll(domain.getStoragePools());
    }
    domainThrift.setStoragePoolIds(storagePoolList);
    domainThrift.setLogicalSpace(domain.getLogicalSpace());
    domainThrift.setFreeSpace(domain.getFreeSpace());

    return domainThrift;
  }

  public static Domain buildDomainFrom(DomainThrift domainThrift) {
    Domain domain = new Domain();
    if (domainThrift == null) {
      return domain;
    }

    domain.setDomainId(domainThrift.getDomainId());
    domain.setDomainName(domainThrift.getDomainName());
    domain.setDomainDescription(domainThrift.getDomainDescription());
    if (domainThrift.isSetStatus()) {
      domain.setStatus(converStatusFromThrift(domainThrift.getStatus()));
    }
    domain.setLastUpdateTime(domainThrift.getLastUpdateTime());
    Set<Long> dataNodSet = new HashSet<>();
    if (domainThrift.isSetDatanodes() && !domainThrift.getDatanodes().isEmpty()) {
      dataNodSet.addAll(domainThrift.getDatanodes());
    }
    domain.setDataNodes(dataNodSet);

    Set<Long> storagePoolIds = new HashSet<>();
    if (domainThrift.isSetStoragePoolIds() && !domainThrift.getStoragePoolIds().isEmpty()) {
      storagePoolIds.addAll(domainThrift.getStoragePoolIds());
    }
    domain.setStoragePools(storagePoolIds);
    domain.setLogicalSpace(domainThrift.getLogicalSpace());
    domain.setFreeSpace(domainThrift.getFreeSpace());
    return domain;
  }

  public static List<DomainThrift> buildDomainThriftListFrom(List<Domain> domains) {
    List<DomainThrift> domainsThrift = new ArrayList<>();
    if (domains == null) {
      return domainsThrift;
    }
    for (Domain domain : domains) {
      domainsThrift.add(RequestResponseHelper.buildDomainThriftFrom(domain));
    }
    return domainsThrift;
  }

  public static InstanceMetadataThrift buildInstanceMetadataThriftFrom(Instance instance) {
    InstanceMetadataThrift instanceMetadataThrift = new InstanceMetadataThrift();
    instanceMetadataThrift.setInstanceId(instance.getId().getId());
    instanceMetadataThrift.setEndpoint(instance.getEndPoint().getHostName());
    instanceMetadataThrift.setGroup(buildThriftGroupFrom(instance.getGroup()));
    return instanceMetadataThrift;
  }

  public static InstanceMetadataThrift buildInstanceMetadataThriftFrom(
      InstanceMetadata instanceMetadata)
      throws DatanodeTypeNotSetExceptionThrift {
    InstanceMetadataThrift instanceMetadataThrift = new InstanceMetadataThrift();
    instanceMetadataThrift.setInstanceId(instanceMetadata.getInstanceId().getId());
    instanceMetadataThrift.setEndpoint(instanceMetadata.getEndpoint());
    instanceMetadataThrift.setGroup(buildThriftGroupFrom(instanceMetadata.getGroup()));
    instanceMetadataThrift
        .setDatanodeType(
            convertDatanodeType2DatanodeTypeThrift(instanceMetadata.getDatanodeType()));
    instanceMetadataThrift
        .setDatanodeStatus(
            DatanodeStatusThrift.valueOf(instanceMetadata.getDatanodeStatus().name()));
    return instanceMetadataThrift;
  }

  // public static SegmentMembership buildMembershipFromPBMembership(PBMembership pbMembership) {
  // List<InstanceId> secondaries = new ArrayList<InstanceId>();
  // List<InstanceId> arbiters = new ArrayList<InstanceId>();
  // List<InstanceId> joiningSecondaries = new ArrayList<InstanceId>();
  // List<InstanceId> inactiveSecondaries = new ArrayList<InstanceId>();
  // for (Long instanceId : pbMembership.getSecondariesList()) {
  // secondaries.add(new InstanceId(instanceId));
  // }
  // for (Long instanceId : pbMembership.getArbitersList()) {
  // arbiters.add(new InstanceId(instanceId));
  // }
  // for (Long instanceId : pbMembership.getJoiningSecondariesList()) {
  // joiningSecondaries.add(new InstanceId(instanceId));
  // }
  // for (Long instanceId : pbMembership.getInactiveSecondariesList()) {
  // inactiveSecondaries.add(new InstanceId(instanceId));
  // }
  // SegmentVersion segmentVersion = new SegmentVersion(pbMembership.getEpoch(), pbMembership
  // .getGeneration());
  // SegmentMembership membership = new SegmentMembership(segmentVersion, new InstanceId
  // (pbMembership.getPrimary()),
  // secondaries, arbiters, inactiveSecondaries, joiningSecondaries);
  // return membership;
  // }

  // public static PBMembership buildPBMembershipFromMembership(SegmentMembership membership) {
  // PBMembership.Builder builder = PBMembership.newBuilder();
  // builder.setEpoch(membership.getSegmentVersion().getEpoch());
  // builder.setGeneration(membership.getSegmentVersion().getGeneration());
  // builder.setPrimary(membership.getPrimary().getId());
  // for (InstanceId instanceId : membership.getSecondaries()) {
  // builder.addSecondaries(instanceId.getId());
  // }
  // for (InstanceId instanceId : membership.getArbiters()) {
  // builder.addArbiters(instanceId.getId());
  // }
  // for (InstanceId instanceId : membership.getJoiningSecondaries()) {
  // builder.addJoiningSecondaries(instanceId.getId());
  // }
  // for (InstanceId instanceId : membership.getInactiveSecondaries()) {
  // builder.addInactiveSecondaries(instanceId.getId());
  // }
  //
  // return builder.build();
  // }

  // public static ReadUnitResultThrift convertResultToThriftResult(PBIOUnitResult readUnitResult) {
  // ReadUnitResultThrift resultThrift = null;
  // if (readUnitResult == PBIOUnitResult.OK) {
  // resultThrift = ReadUnitResultThrift.OK;
  // } else if (readUnitResult == PBIOUnitResult.OutOfRange) {
  // resultThrift = ReadUnitResultThrift.OutOfRange;
  // } else if (readUnitResult == PBIOUnitResult.ChecksumMismatched) {
  // resultThrift = ReadUnitResultThrift.ChecksumMismatched;
  // } else {
  // logger.error("unknown status: {}", readUnitResult);
  // }
  // return resultThrift;
  // }

  // public static BroadcastLogStatus convertPBStatusToStatus(PBBroadcastLogStatus pbLogStatus) {
  // BroadcastLogStatus convertStatus = null;
  // Validate.notNull(pbLogStatus, "can not be null status");
  // if (pbLogStatus == PBBroadcastLogStatus.Committed) {
  // convertStatus = BroadcastLogStatus.Committed;
  // } else if (pbLogStatus == PBBroadcastLogStatus.Created) {
  // convertStatus = BroadcastLogStatus.Created;
  // } else if (pbLogStatus == PBBroadcastLogStatus.Creating) {
  // convertStatus = BroadcastLogStatus.Creating;
  // } else if (pbLogStatus == PBBroadcastLogStatus.Abort) {
  // convertStatus = BroadcastLogStatus.Abort;
  // } else if (pbLogStatus == PBBroadcastLogStatus.AbortConfirmed) {
  // convertStatus = BroadcastLogStatus.AbortConfirmed;
  // } else {
  // logger.warn("don't know this status: {}", pbLogStatus.name());
  // }
  // return convertStatus;
  // }

  public static boolean isFinalStatus(BroadcastLogStatus pbLogStatus) {
    return pbLogStatus == BroadcastLogStatus.Committed
        || pbLogStatus == BroadcastLogStatus.AbortConfirmed;
  }

  public static boolean isFinalStatus(BroadcastLogStatusThrift thriftLogStatus) {
    return thriftLogStatus == BroadcastLogStatusThrift.Committed
        || thriftLogStatus == BroadcastLogStatusThrift.AbortConfirmed;
  }

  // public static BroadcastLogStatusThrift convertPBStatusToThriftStatus(PBBroadcastLogStatus
  // pbLogStatus) {
  // BroadcastLogStatusThrift convertStatus = null;
  // Validate.notNull(pbLogStatus, "can not be null status");
  // if (pbLogStatus == PBBroadcastLogStatus.Committed) {
  // convertStatus = BroadcastLogStatusThrift.Committed;
  // } else if (pbLogStatus == PBBroadcastLogStatus.Created) {
  // convertStatus = BroadcastLogStatusThrift.Created;
  // } else if (pbLogStatus == PBBroadcastLogStatus.Creating) {
  // convertStatus = BroadcastLogStatusThrift.Creating;
  // } else if (pbLogStatus == PBBroadcastLogStatus.Abort) {
  // convertStatus = BroadcastLogStatusThrift.Abort;
  // } else if (pbLogStatus == PBBroadcastLogStatus.AbortConfirmed) {
  // convertStatus = BroadcastLogStatusThrift.AbortConfirmed;
  // } else {
  // logger.warn("don't know this status: {}", pbLogStatus.name());
  // }
  // return convertStatus;
  // }

  public static BroadcastLogStatusThrift convertStatusToThriftStatus(BroadcastLogStatus logStatus) {
    BroadcastLogStatusThrift logStatusThrift = null;
    Validate.notNull(logStatus, "can not be null status");
    if (logStatus == BroadcastLogStatus.Committed) {
      logStatusThrift = BroadcastLogStatusThrift.Committed;
    } else if (logStatus == BroadcastLogStatus.Created) {
      logStatusThrift = BroadcastLogStatusThrift.Created;
    } else if (logStatus == BroadcastLogStatus.Creating) {
      logStatusThrift = BroadcastLogStatusThrift.Creating;
    } else if (logStatus == BroadcastLogStatus.Abort) {
      logStatusThrift = BroadcastLogStatusThrift.Abort;
    } else if (logStatus == BroadcastLogStatus.AbortConfirmed) {
      logStatusThrift = BroadcastLogStatusThrift.AbortConfirmed;
    } else {
      logger.warn("don't know this status: {}", logStatus.name());
    }
    return logStatusThrift;
  }

  // public static PBBroadcastLogStatus convertStatusToPBStatus(BroadcastLogStatus logStatus) {
  // PBBroadcastLogStatus pbLogStatus = null;
  // Validate.notNull(logStatus, "can not be null status");
  // if (logStatus == BroadcastLogStatus.Committed) {
  // pbLogStatus = PBBroadcastLogStatus.Committed;
  // } else if (logStatus == BroadcastLogStatus.Created) {
  // pbLogStatus = PBBroadcastLogStatus.Created;
  // } else if (logStatus == BroadcastLogStatus.Creating) {
  // pbLogStatus = PBBroadcastLogStatus.Creating;
  // } else if (logStatus == BroadcastLogStatus.Abort) {
  // pbLogStatus = PBBroadcastLogStatus.Abort;
  // } else if (logStatus == BroadcastLogStatus.AbortConfirmed) {
  // pbLogStatus = PBBroadcastLogStatus.AbortConfirmed;
  // } else {
  // logger.warn("don't know this status: {}", logStatus.name());
  // }
  // return pbLogStatus;
  // }

  public static BroadcastLogStatus convertThriftStatusToStatus(
      BroadcastLogStatusThrift logStatusThrift) {
    BroadcastLogStatus logStatus = null;
    Validate.notNull(logStatusThrift, "can not be null status");
    if (logStatusThrift == BroadcastLogStatusThrift.Committed) {
      logStatus = BroadcastLogStatus.Committed;
    } else if (logStatusThrift == BroadcastLogStatusThrift.Created) {
      logStatus = BroadcastLogStatus.Created;
    } else if (logStatusThrift == BroadcastLogStatusThrift.Creating) {
      logStatus = BroadcastLogStatus.Creating;
    } else if (logStatusThrift == BroadcastLogStatusThrift.Abort) {
      logStatus = BroadcastLogStatus.Abort;
    } else if (logStatusThrift == BroadcastLogStatusThrift.AbortConfirmed) {
      logStatus = BroadcastLogStatus.AbortConfirmed;
    } else {
      logger.warn("don't know this status: {}", logStatusThrift);
    }
    return logStatus;
  }

  // public static PBWriteResponseUnit buildPBWriteResponseUnitFrom(PBWriteRequestUnit writeUnit,
  // long logId,
  // PBIOUnitResult errorCode) {
  // PBWriteResponseUnit.Builder builder = PBWriteResponseUnit.newBuilder();
  // builder.setLogId(logId);
  // builder.setLogUUID(writeUnit.getLogUUID());
  // builder.setOffset(writeUnit.getOffset());
  // builder.setLength(writeUnit.getLength());
  // builder.setLogResult(errorCode);
  // return builder.build();
  // }

  // public static PBWriteResponseUnit buildPBWriteResponseUnitFrom(PBWriteRequestUnit writeUnit,
  // PBIOUnitResult errorCode) {
  // PBWriteResponseUnit.Builder builder = PBWriteResponseUnit.newBuilder();
  // builder.setLogId(writeUnit.getLogId());
  // builder.setLogUUID(writeUnit.getLogUUID());
  // builder.setOffset(writeUnit.getOffset());
  // builder.setLength(writeUnit.getLength());
  // builder.setLogResult(errorCode);
  // return builder.build();
  // }

  // public static PBReadResponseUnit buildPBReadResponseUnitFrom(PBReadRequestUnit readRequestUnit,
  // PBIOUnitResult errorCode) {
  // PBReadResponseUnit.Builder builder = PBReadResponseUnit.newBuilder();
  // builder.setRequestId(readRequestUnit.getRequestId());
  // builder.setOffset(readRequestUnit.getOffset());
  // builder.setLength(readRequestUnit.getLength());
  // builder.setResult(errorCode);
  // return builder.build();
  // }

  // public static PBReadResponseUnit buildPBReadResponseUnitFrom(PBReadRequestUnit
  // readRequestUnit, byte[] data,
  // long checksum) {
  // return buildPBReadResponseUnitFrom(readRequestUnit, data, 0, data.length, checksum);
  // }

  // public static PBReadResponseUnit buildPBReadResponseUnitFrom(PBReadRequestUnit
  // readRequestUnit, byte[] data,
  // int offset, int len, long checksum) {
  // PBReadResponseUnit.Builder builder = PBReadResponseUnit.newBuilder();
  // builder.setRequestId(readRequestUnit.getRequestId());
  // builder.setOffset(readRequestUnit.getOffset());
  // builder.setLength(readRequestUnit.getLength());
  // builder.setResult(PBIOUnitResult.OK);
  // builder.setData(ByteStringUtils.newInstance(data, offset, len));
  // builder.setChecksum(checksum);
  // return builder.build();
  // }

  // public static PBReadResponseUnit buildZeroBufferForPBReadResponseUnit(PBReadRequestUnit
  // readRequestUnit) {
  // PBReadResponseUnit.Builder builder = PBReadResponseUnit.newBuilder();
  // builder.setRequestId(readRequestUnit.getRequestId());
  // builder.setOffset(readRequestUnit.getOffset());
  // builder.setLength(readRequestUnit.getLength());
  // builder.setResult(PBIOUnitResult.Free);
  //
  // byte[] data = new byte[readRequestUnit.getLength()];
  // Arrays.fill(data, (byte) 0);
  // long checksum = NetworkChecksumHelper.computeChecksum(data, 0, data.length);
  // builder.setData(ByteStringUtils.newInstance(data, 0, data.length));
  // builder.setChecksum(checksum);
  //
  // return builder.build();
  // }

  public static StoragePoolStrategy convertStrategyFromThrift(
      StoragePoolStrategyThrift thriftStatus) {
    StoragePoolStrategy strategy = null;
    if (thriftStatus == StoragePoolStrategyThrift.Capacity) {
      strategy = StoragePoolStrategy.Capacity;
    } else if (thriftStatus == StoragePoolStrategyThrift.Performance) {
      strategy = StoragePoolStrategy.Performance;
    } else if (thriftStatus == StoragePoolStrategyThrift.Mixed) {
      strategy = StoragePoolStrategy.Mixed;
    } else {
      Validate.isTrue(false, "unknown strategy from thrift: " + thriftStatus);
    }
    return strategy;
  }

  public static ScsiDescriptionTypeThrift convertScsiDescriptionTpyeThrift(String descriptionTpye) {
    ScsiDescriptionTypeThrift scsiDescriptionTpyeThrift = null;
    if (ScsiDescriptionTypeThrift.LaunchDriver.name().equals(descriptionTpye)) {
      scsiDescriptionTpyeThrift = ScsiDescriptionTypeThrift.LaunchDriver;
    } else if (ScsiDescriptionTypeThrift.UmountDriver.name().equals(descriptionTpye)) {
      scsiDescriptionTpyeThrift = ScsiDescriptionTypeThrift.UmountDriver;
    }
    return scsiDescriptionTpyeThrift;
  }

  public static StoragePoolStrategyThrift convertThriftStrategyFrom(StoragePoolStrategy strategy) {
    StoragePoolStrategyThrift strategyThrift = null;
    if (strategy == StoragePoolStrategy.Capacity) {
      strategyThrift = StoragePoolStrategyThrift.Capacity;
    } else if (strategy == StoragePoolStrategy.Performance) {
      strategyThrift = StoragePoolStrategyThrift.Performance;
    } else if (strategy == StoragePoolStrategy.Mixed) {
      strategyThrift = StoragePoolStrategyThrift.Mixed;
    } else {
      Validate.isTrue(false, "unknown strategy: " + strategy);
    }
    return strategyThrift;
  }

  public static Status converStatusFromThrift(StatusThrift statusThrift) {
    Status status = null;
    if (statusThrift == StatusThrift.Available) {
      status = Status.Available;
    } else if (statusThrift == StatusThrift.Deleting) {
      status = Status.Deleting;
    } else {
      Validate.isTrue(false, "unknown Thrift status: " + statusThrift);
    }
    return status;
  }

  public static StatusThrift converThriftStatusFrom(Status status) {
    StatusThrift statusThrift = null;
    if (status == Status.Available) {
      statusThrift = StatusThrift.Available;
    } else if (status == Status.Deleting) {
      statusThrift = StatusThrift.Deleting;
    } else {
      Validate.isTrue(false, "unknown status: " + status);
    }
    return statusThrift;
  }

  public static StoragePool buildStoragePoolFromThrift(StoragePoolThrift poolThrift) {
    StoragePool storagePool = new StoragePool();

    storagePool.setDomainId(poolThrift.getDomainId());
    storagePool.setDomainName(poolThrift.getDomainName());
    storagePool.setPoolId(poolThrift.getPoolId());
    storagePool.setName(poolThrift.getPoolName());
    storagePool.setStoragePoolLevel(poolThrift.getStoragePoolLevel());
    storagePool.setStrategy(convertStrategyFromThrift(poolThrift.getStrategy()));
    if (poolThrift.isSetStatus()) {
      storagePool.setStatus(converStatusFromThrift(poolThrift.getStatus()));
    }
    storagePool.setLastUpdateTime(poolThrift.getLastUpdateTime());

    if (poolThrift.isSetDescription() && poolThrift.getDescription() != null) {
      storagePool.setDescription(poolThrift.getDescription());
    }
    if (poolThrift.isSetArchivesInDatanode() && poolThrift.getArchivesInDatanode() != null
        && !poolThrift
        .getArchivesInDatanode().isEmpty()) {
      Multimap<Long, Long> archivesInDataNode = Multimaps
          .synchronizedSetMultimap(HashMultimap.<Long, Long>create());
      for (Entry<Long, Set<Long>> entry : poolThrift.getArchivesInDatanode().entrySet()) {
        archivesInDataNode.putAll(entry.getKey(), entry.getValue());
      }
      storagePool.setArchivesInDataNode(archivesInDataNode);
    }
    if (poolThrift.isSetVolumeIds() && poolThrift.getVolumeIds() != null && !poolThrift
        .getVolumeIds().isEmpty()) {
      Set<Long> volumeIds = new HashSet<>();
      volumeIds.addAll(poolThrift.getVolumeIds());
      storagePool.setVolumeIds(volumeIds);
    }
    storagePool.setMigrationSpeed(poolThrift.getMigrationSpeed());
    storagePool.setMigrationRatio(poolThrift.getMigrationRatio());
    storagePool.setTotalSpace(poolThrift.getTotalSpace());
    storagePool.setFreeSpace(poolThrift.getFreeSpace());
    storagePool.setLogicalPssFreeSpace(poolThrift.getLogicalPssFreeSpace());
    storagePool.setLogicalPsaFreeSpace(poolThrift.getLogicalPsaFreeSpace());

    if (poolThrift.isSetMigrationRule()) {
      storagePool.setMigrationRuleId(poolThrift.getMigrationRule().getRuleId());
    } else {
      storagePool.setMigrationRuleId(StoragePool.DEFAULT_MIGRATION_RULE_ID);
    }
    storagePool.setTotalMigrateDataSizeMb(poolThrift.getTotalMigrateDataSizeMb());

    return storagePool;
  }

  public static StoragePoolThrift buildThriftStoragePoolFrom(StoragePool storagePool,
      MigrationRule migrationRule) {
    StoragePoolThrift poolThrift = new StoragePoolThrift();
    poolThrift.setDomainId(storagePool.getDomainId());
    poolThrift.setDomainName(storagePool.getDomainName());
    poolThrift.setPoolId(storagePool.getPoolId());
    poolThrift.setPoolName(storagePool.getName());
    poolThrift.setStoragePoolLevel(storagePool.getStoragePoolLevel());
    poolThrift.setStrategy(convertThriftStrategyFrom(storagePool.getStrategy()));
    poolThrift.setStatus(converThriftStatusFrom(storagePool.getStatus()));
    poolThrift.setLastUpdateTime(storagePool.getLastUpdateTime());

    if (storagePool.getDescription() != null) {
      poolThrift.setDescription(storagePool.getDescription());
    }

    if (!storagePool.getArchivesInDataNode().isEmpty()) {
      Map<Long, Set<Long>> tempMap = new HashMap<>();
      Set<Long> keySet = new HashSet<>();
      keySet.addAll(storagePool.getArchivesInDataNode().keySet());
      for (Long key : keySet) {
        Set<Long> valueList = new HashSet<>();
        valueList.addAll(storagePool.getArchivesInDataNode().get(key));
        tempMap.put(key, valueList);
      }
      poolThrift.setArchivesInDatanode(tempMap);
    }
    if (!storagePool.getVolumeIds().isEmpty()) {
      Set<Long> volumeIds = new HashSet<>();
      volumeIds.addAll(storagePool.getVolumeIds());
      poolThrift.setVolumeIds(volumeIds);
    }

    if (migrationRule != null) {
      MigrationRuleThrift migrationRuleThrift = buildMigrationRuleThriftFrom(migrationRule);
      poolThrift.setMigrationRule(migrationRuleThrift);
    }

    poolThrift.setMigrationSpeed(storagePool.getMigrationSpeed());
    poolThrift.setMigrationRatio(storagePool.getMigrationRatio());

    poolThrift.setTotalSpace(storagePool.getTotalSpace());
    poolThrift.setFreeSpace(storagePool.getFreeSpace());
    poolThrift.setLogicalPssFreeSpace(storagePool.getLogicalPssFreeSpace());
    poolThrift.setLogicalPsaFreeSpace(storagePool.getLogicalPsaFreeSpace());
    poolThrift.setTotalMigrateDataSizeMb(storagePool.getTotalMigrateDataSizeMb());
    return poolThrift;
  }

  public static TotalAndUsedCapacityThrift buildThriftTotalAndUsedCapacity(
      TotalAndUsedCapacity capacity) {
    TotalAndUsedCapacityThrift capacityThrift = new TotalAndUsedCapacityThrift();
    capacityThrift.setTotalCapacity(capacity.getTotalCapacity());
    capacityThrift.setUsedCapacity(capacity.getUsedCapacity());
    return capacityThrift;
  }

  public static TotalAndUsedCapacity buildTotalAndUsedCapacityFrom(
      TotalAndUsedCapacityThrift capacityThrift) {
    TotalAndUsedCapacity capacity = new TotalAndUsedCapacity();
    capacity.setTotalCapacity(capacityThrift.getTotalCapacity());
    capacity.setUsedCapacity(capacityThrift.getUsedCapacity());
    return capacity;
  }

  public static CapacityRecordThrift buildThriftCapacityRecordFrom(CapacityRecord capacityRecord) {
    CapacityRecordThrift capacityRecordThrift = new CapacityRecordThrift();
    Map<String, TotalAndUsedCapacityThrift> capacityRecordThriftMap = new HashMap<>();

    for (Entry<String, TotalAndUsedCapacity> entry : capacityRecord.getRecordMap().entrySet()) {
      String date = entry.getKey();
      TotalAndUsedCapacity capacity = entry.getValue();
      TotalAndUsedCapacityThrift capacityThrift = buildThriftTotalAndUsedCapacity(capacity);
      capacityRecordThriftMap.put(date, capacityThrift);
    }

    capacityRecordThrift.setCapacityRecordMap(capacityRecordThriftMap);
    return capacityRecordThrift;
  }

  public static CapacityRecord buildCapacityRecordFrom(CapacityRecordThrift capacityRecordThrift) {
    CapacityRecord capacityRecord = new CapacityRecord();
    Map<String, TotalAndUsedCapacity> capacityRecordMap = new HashMap<>();

    for (Entry<String, TotalAndUsedCapacityThrift> entry : capacityRecordThrift
        .getCapacityRecordMap()
        .entrySet()) {
      String date = entry.getKey();
      TotalAndUsedCapacityThrift capacityThrift = entry.getValue();
      TotalAndUsedCapacity capacity = buildTotalAndUsedCapacityFrom(capacityThrift);
      capacityRecordMap.put(date, capacity);
    }
    capacityRecord.setRecordMap(capacityRecordMap);
    return capacityRecord;
  }

  public static SegmentUnitType convertFromSegmentUnitTypeThrift(
      SegmentUnitTypeThrift segmentUnitTypeThrift) {
    SegmentUnitType segmentUnitType = null;
    if (segmentUnitTypeThrift == SegmentUnitTypeThrift.Arbiter) {
      segmentUnitType = SegmentUnitType.Arbiter;
    } else if (segmentUnitTypeThrift == SegmentUnitTypeThrift.Normal) {
      segmentUnitType = SegmentUnitType.Normal;
    } else if (segmentUnitTypeThrift == SegmentUnitTypeThrift.Flexible) {
      segmentUnitType = SegmentUnitType.Flexible;
    } else if (segmentUnitTypeThrift != null) {
      Validate.isTrue(false, "unknown segment unit type thrift: " + segmentUnitTypeThrift);
    }
    return segmentUnitType;
  }

  public static RebalanceTaskThrift buildRebalanceTaskThrift(RebalanceTask rebalanceTask) {
    RebalanceTaskThrift rebalanceTaskThrift = new RebalanceTaskThrift();
    rebalanceTaskThrift.setTaskId(rebalanceTask.getTaskId());
    rebalanceTaskThrift.setDestInstanceId(rebalanceTask.getDestInstanceId().getId());
    rebalanceTaskThrift.setSourceSegmentUnit(
        buildThriftSegUnitMetadataFrom(rebalanceTask.getSourceSegmentUnit()));
    rebalanceTaskThrift.setTaskType(convertRebalanceTaskType(rebalanceTask.getTaskType()));
    return rebalanceTaskThrift;
  }

  public static RebalanceTask buildRebalanceTask(RebalanceTaskThrift rebalanceTaskThrift) {
    InstanceId instanceToMigrateTo = new InstanceId(rebalanceTaskThrift.getDestInstanceId());
    SegmentUnitMetadata segmentUnitToRemove = buildSegmentUnitMetadataFrom(
        rebalanceTaskThrift.getSourceSegmentUnit());
    return new RebalanceTask(rebalanceTaskThrift.getTaskId(), segmentUnitToRemove,
        instanceToMigrateTo,
        convertRebalanceTaskType(rebalanceTaskThrift.getTaskType()));
  }

  public static RebalanceTaskTypeThrift convertRebalanceTaskType(
      RebalanceTask.RebalanceTaskType type) {
    if (type == RebalanceTask.RebalanceTaskType.PrimaryRebalance) {
      return RebalanceTaskTypeThrift.PrimaryRebalance;
    } else if (type == RebalanceTask.RebalanceTaskType.NormalRebalance) {
      return RebalanceTaskTypeThrift.NormalRebalance;
    } else {
      throw new IllegalArgumentException();
    }
  }

  public static RebalanceTask.RebalanceTaskType convertRebalanceTaskType(
      RebalanceTaskTypeThrift typeThrift) {
    if (typeThrift == RebalanceTaskTypeThrift.PrimaryRebalance) {
      return RebalanceTask.RebalanceTaskType.PrimaryRebalance;
    } else if (typeThrift == RebalanceTaskTypeThrift.NormalRebalance) {
      return RebalanceTask.RebalanceTaskType.NormalRebalance;
    } else {
      throw new IllegalArgumentException();
    }
  }

  // VolumeAccess

  public static VolumeAccessRule buildVolumeAccessRuleFrom(
      VolumeAccessRuleThrift volumeAccessRuleFromRemote) {
    VolumeAccessRule volumeAccessRule = new VolumeAccessRule();
    volumeAccessRule.setRuleId(volumeAccessRuleFromRemote.getRuleId());
    volumeAccessRule.setIncommingHostName(volumeAccessRuleFromRemote.getIncomingHostName());
    volumeAccessRule.setPermission(
        AccessPermissionType.valueOf(volumeAccessRuleFromRemote.getPermission().name()));
    if (volumeAccessRuleFromRemote.isSetStatus()) {
      volumeAccessRule
          .setStatus(AccessRuleStatus.findByName(volumeAccessRuleFromRemote.getStatus().name()));
    }
    return volumeAccessRule;
  }

  public static VolumeAccessRuleThrift buildVolumeAccessRuleThriftFrom(
      VolumeAccessRule volumeAccessRule) {
    VolumeAccessRuleThrift volumeAccessRuleToRemote = new VolumeAccessRuleThrift();
    volumeAccessRuleToRemote.setRuleId(volumeAccessRule.getRuleId());
    volumeAccessRuleToRemote.setIncomingHostName(volumeAccessRule.getIncommingHostName());
    volumeAccessRuleToRemote
        .setPermission(AccessPermissionTypeThrift.valueOf(volumeAccessRule.getPermission().name()));
    volumeAccessRuleToRemote
        .setStatus(AccessRuleStatusThrift.valueOf(volumeAccessRule.getStatus().name()));
    return volumeAccessRuleToRemote;
  }

  public static Volume2AccessRuleRelationship buildVolume2AccessRuleRelationshipFromThrift(
      VolumeRuleRelationshipThrift volume2RuleThrift) {
    Volume2AccessRuleRelationship volume2Rule = new Volume2AccessRuleRelationship();
    volume2Rule.setVolumeId(volume2RuleThrift.getVolumeId());
    volume2Rule.setRelationshipId(volume2RuleThrift.getRelationshipId());
    volume2Rule.setRuleId(volume2RuleThrift.getRuleId());
    volume2Rule.setStatus(
        AccessRuleStatusBindingVolume
            .findByName(volume2RuleThrift.getAccessRuleStatusBindingVolume().name()));
    return volume2Rule;
  }

  public static VolumeRuleRelationshipThrift buildThriftVolumeRuleRelationship(
      Volume2AccessRuleRelationship volume2Rule) {
    VolumeRuleRelationshipThrift volume2RuleThrift = new VolumeRuleRelationshipThrift();
    volume2RuleThrift.setVolumeId(volume2Rule.getVolumeId());
    volume2RuleThrift.setRelationshipId(volume2Rule.getRelationshipId());
    volume2RuleThrift.setRuleId(volume2Rule.getRuleId());
    volume2RuleThrift.setAccessRuleStatusBindingVolume(
        AccessRuleStatusBindingVolumeThrift.findByValue(volume2Rule.getStatus().getValue()));
    return volume2RuleThrift;
  }

  // IoLimitation

  public static IoLimitationThrift buildThriftIoLimitationFrom(IoLimitation ioLimitation) {
    Validate.notNull(ioLimitation);
    IoLimitationThrift ioLimitationThrift = new IoLimitationThrift();
    ioLimitationThrift.setLimitationId(ioLimitation.getId());
    ioLimitationThrift.setLimitationName(ioLimitation.getName());

    if (ioLimitation.getLimitType() != null) {
      ioLimitationThrift.setLimitType(convertLimitType(ioLimitation.getLimitType()));
    } else {
      logger.error("cannot get limit type form ioLimitation: {}", ioLimitation);
    }

    if (ioLimitation.getStatus() != null) {
      ioLimitationThrift
          .setStatus(IoLimitationStatusThrift.valueOf(ioLimitation.getStatus().name()));
    } else {
      logger.error("cannot get limit status form ioLimitation: {}", ioLimitation);
    }

    List<IoLimitationEntryThrift> entryThrifts = new ArrayList<>();
    if (ioLimitation.getLimitType() == IoLimitation.LimitType.Dynamic) {
      logger.debug("buildThriftIoLimitationFrom {}", IoLimitation.LimitType.Dynamic);
      List<IoLimitationEntry> ioLimitationEntries = ioLimitation.getEntries();
      for (IoLimitationEntry ioLimitationEntry : ioLimitationEntries) {
        IoLimitationEntryThrift entryThrift = new IoLimitationEntryThrift();
        entryThrift.setEntryId(ioLimitationEntry.getEntryId());
        entryThrift.setStartTime(ioLimitationEntry.getStartTimeWithLocalFormat().toString());
        entryThrift.setEndTime(ioLimitationEntry.getEndTimeWithLocalFormat().toString());
        entryThrift.setLowerLimitedIoPs(ioLimitationEntry.getLowerLimitedIops());
        entryThrift.setUpperLimitedIoPs(ioLimitationEntry.getUpperLimitedIops());
        entryThrift.setLowerLimitedThroughput(ioLimitationEntry.getLowerLimitedThroughput());
        entryThrift.setUpperLimitedThroughput(ioLimitationEntry.getUpperLimitedThroughput());

        entryThrifts.add(entryThrift);
      }

    } else if (ioLimitation.getLimitType() == IoLimitation.LimitType.Static) {
      logger.debug("buildThriftIoLimitationFrom {}", IoLimitation.LimitType.Static);

      List<IoLimitationEntry> ioLimitationEntries = ioLimitation.getEntries();
      for (IoLimitationEntry ioLimitationEntry : ioLimitationEntries) {
        IoLimitationEntryThrift entryThrift = new IoLimitationEntryThrift();
        entryThrift.setEntryId(ioLimitationEntry.getEntryId());
        entryThrift.setLowerLimitedIoPs(ioLimitationEntry.getLowerLimitedIops());
        entryThrift.setUpperLimitedIoPs(ioLimitationEntry.getUpperLimitedIops());
        entryThrift.setLowerLimitedThroughput(ioLimitationEntry.getLowerLimitedThroughput());
        entryThrift.setUpperLimitedThroughput(ioLimitationEntry.getUpperLimitedThroughput());
        entryThrift.setStartTime(LocalTime.of(00, 00, 00).toString());
        entryThrift.setEndTime(LocalTime.of(23, 59, 59).toString());

        entryThrifts.add(entryThrift);
      }
    }

    logger.debug("buildThriftIoLimitationFrom {}", entryThrifts);
    ioLimitationThrift.setEntries(entryThrifts);

    return ioLimitationThrift;
  }

  public static IoLimitation buildIoLimitationFrom(IoLimitationThrift ioLimitationThrift) {
    Validate.notNull(ioLimitationThrift);
    IoLimitation ioLimitation = new IoLimitation();
    ioLimitation.setId(ioLimitationThrift.getLimitationId());
    ioLimitation.setName(ioLimitationThrift.getLimitationName());

    IoLimitation.LimitType type = convertLimitType(ioLimitationThrift.getLimitType());
    ioLimitation.setLimitType(type);

    if (ioLimitationThrift.getStatus() != null) {
      ioLimitation.setStatus(IoLimitationStatus.valueOf(ioLimitationThrift.getStatus().name()));
    }

    logger.debug("buildIoLimitationFrom {}", ioLimitationThrift.getEntries());

    List<IoLimitationEntry> entries = new ArrayList<>();
    if (type == IoLimitation.LimitType.Dynamic) {
      logger.debug("buildIoLimitationFrom {}", IoLimitation.LimitType.Dynamic);
      List<IoLimitationEntryThrift> entriesThrift = ioLimitationThrift.getEntries();
      for (IoLimitationEntryThrift entryThrift : entriesThrift) {
        IoLimitationEntry entry = new IoLimitationEntry();
        entry.setEntryId(entryThrift.getEntryId());
        entry.setStartTime(entryThrift.getStartTime());
        entry.setEndTime(entryThrift.getEndTime());
        entry.setLowerLimitedIops(entryThrift.getLowerLimitedIoPs());
        entry.setUpperLimitedIops(entryThrift.getUpperLimitedIoPs());
        entry.setLowerLimitedThroughput(entryThrift.getLowerLimitedThroughput());
        entry.setUpperLimitedThroughput(entryThrift.getUpperLimitedThroughput());

        entries.add(entry);
      }
      logger.debug("buildIoLimitationFrom entries {}", entries);

    } else if (type == IoLimitation.LimitType.Static) {
      logger.debug("buildIoLimitationFrom {}", IoLimitation.LimitType.Static);
      List<IoLimitationEntryThrift> entriesThrift = ioLimitationThrift.getEntries();
      for (IoLimitationEntryThrift entrieThrift : entriesThrift) {
        IoLimitationEntry entry = new IoLimitationEntry();
        entry.setEntryId(entrieThrift.getEntryId());
        entry.setLowerLimitedIops(entrieThrift.getLowerLimitedIoPs());
        entry.setUpperLimitedIops(entrieThrift.getUpperLimitedIoPs());
        entry.setLowerLimitedThroughput(entrieThrift.getLowerLimitedThroughput());
        entry.setUpperLimitedThroughput(entrieThrift.getUpperLimitedThroughput());
        entry.setStartTime(LocalTime.of(00, 00, 00).toString());
        entry.setEndTime(LocalTime.of(23, 59, 59).toString());
        entries.add(entry);
      }
      logger.debug("buildIoLimitationFrom  entries {}", entries);
    }
    ioLimitation.setEntries(entries);

    return ioLimitation;
  }

  public static IoLimitationRelationship buildIoLimitationRelationshipFromThrift(
      IoLimitationRelationShipThrift ioLimitationRelationThrift) {
    IoLimitationRelationship ioLimitationRelationship = new IoLimitationRelationship();
    ioLimitationRelationship.setRelationshipId(ioLimitationRelationThrift.getRelationshipId());
    ioLimitationRelationship.setRuleId(ioLimitationRelationThrift.getRuleId());
    ioLimitationRelationship.setVolumeId(ioLimitationRelationship.getVolumeId());
    ioLimitationRelationship.setDriverContainerId(ioLimitationRelationship.getDriverContainerId());
    ioLimitationRelationship.setDriverType(ioLimitationRelationship.getDriverType());
    ioLimitationRelationship.setSnapshotId(ioLimitationRelationship.getSnapshotId());
    ioLimitationRelationship.setStatus(IoLimitationStatusBindingDrivers
        .findByName(ioLimitationRelationThrift.getIoLimitationStatusBindingDrivers().name()));

    return ioLimitationRelationship;
  }

  public static IoLimitationRelationShipThrift buildThriftIoLimitationRelationShip(
      IoLimitationRelationship ioLimitationRelationship) {
    IoLimitationRelationShipThrift ioLimitationRelationShipThrift =
        new IoLimitationRelationShipThrift();
    ioLimitationRelationShipThrift.setRelationshipId(ioLimitationRelationship.getRelationshipId());
    ioLimitationRelationShipThrift.setRuleId(ioLimitationRelationship.getRuleId());
    ioLimitationRelationShipThrift.setVolumeId(ioLimitationRelationship.getVolumeId());
    ioLimitationRelationShipThrift
        .setDriverContainerId(ioLimitationRelationship.getDriverContainerId());
    ioLimitationRelationShipThrift
        .setDriverType(DriverTypeThrift.valueOf(ioLimitationRelationship.getDriverType().name()));
    ioLimitationRelationShipThrift.setSnapshotId(ioLimitationRelationship.getSnapshotId());
    ioLimitationRelationShipThrift.setIoLimitationStatusBindingDrivers(
        IoLimitationStatusBindingDriversThrift
            .findByValue(ioLimitationRelationship.getStatus().getValue()));
    return ioLimitationRelationShipThrift;
  }

  // MigrationRule

  public static MigrationRule buildMigrationRuleFrom(MigrationRuleThrift migrationRuleFromRemote) {
    MigrationRule migrationRule = new MigrationRule();
    migrationRule.setRuleId(migrationRuleFromRemote.getRuleId());
    migrationRule.setMigrationRuleName(migrationRuleFromRemote.getMigrationRuleName());
    migrationRule.setMaxMigrationSpeed(migrationRuleFromRemote.getMaxMigrationSpeed());
    migrationRule
        .setMigrationStrategy(
            MigrationStrategy.valueOf(migrationRuleFromRemote.getMigrationStrategy().name()));
    if (migrationRuleFromRemote.isSetMigrationRuleStatus()) {
      migrationRule
          .setStatus(MigrationRuleStatus
              .findByName(migrationRuleFromRemote.getMigrationRuleStatus().name()));
    }

    migrationRule.setCheckSecondaryInactiveThresholdMode(
        CheckSecondaryInactiveThresholdMode.valueOf(migrationRuleFromRemote.getMode().name()));
    migrationRule.setStartTime(migrationRuleFromRemote.getStartTime());
    migrationRule.setEndTime(migrationRuleFromRemote.getEndTime());
    migrationRule.setWaitTime(migrationRuleFromRemote.getWaitTime());
    migrationRule.setIgnoreMissPagesAndLogs(migrationRuleFromRemote.isIgnoreMissPagesAndLogs());
    migrationRule.setBuiltInRule(migrationRuleFromRemote.isBuiltInRule());
    return migrationRule;
  }

  public static MigrationRuleThrift buildMigrationRuleThriftFrom(MigrationRule migrationRule) {
    MigrationRuleThrift migrationRuleToRemote = new MigrationRuleThrift();
    migrationRuleToRemote.setRuleId(migrationRule.getRuleId());
    migrationRuleToRemote.setMigrationRuleName(migrationRule.getMigrationRuleName());
    migrationRuleToRemote.setMaxMigrationSpeed(migrationRule.getMaxMigrationSpeed());
    migrationRuleToRemote
        .setMigrationStrategy(
            MigrationStrategyThrift.valueOf(migrationRule.getMigrationStrategy().name()));
    migrationRuleToRemote
        .setMigrationRuleStatus(
            MigrationRuleStatusThrift.valueOf(migrationRule.getStatus().name()));

    migrationRuleToRemote.setStartTime(migrationRule.getStartTime());
    migrationRuleToRemote.setEndTime(migrationRule.getEndTime());
    migrationRuleToRemote.setWaitTime(migrationRule.getWaitTime());
    migrationRuleToRemote.setMode(CheckSecondaryInactiveThresholdModeThrift
        .valueOf(migrationRule.getCheckSecondaryInactiveThresholdMode().name()));
    migrationRuleToRemote.setIgnoreMissPagesAndLogs(migrationRule.isIgnoreMissPagesAndLogs());
    migrationRuleToRemote.setBuiltInRule(migrationRule.isBuiltInRule());
    return migrationRuleToRemote;
  }

  public static MigrationRuleRelationship buildMigrationRuleRelationshipFromThrift(
      MigrationRuleRelationShipThrift migrationSpeedRelationShipThrift) {
    MigrationRuleRelationship migrationRuleRelationship = new MigrationRuleRelationship();
    migrationRuleRelationship
        .setRelationshipId(migrationSpeedRelationShipThrift.getRelationshipId());
    migrationRuleRelationship.setRuleId(migrationSpeedRelationShipThrift.getRuleId());
    migrationRuleRelationship.setStoragePoolId(migrationSpeedRelationShipThrift.getStoragePoolId());
    migrationRuleRelationship.setStatus(MigrationRuleStatusBindingPools
        .findByName(migrationSpeedRelationShipThrift.getMigrationRuleStatusBindingPools().name()));
    return migrationRuleRelationship;
  }

  public static MigrationRuleRelationShipThrift buildThriftMigrationRuleRelationShip(
      MigrationRuleRelationship migrationRuleRelationship) {
    MigrationRuleRelationShipThrift migrationSpeedRelationShipThrift =
        new MigrationRuleRelationShipThrift();
    migrationSpeedRelationShipThrift.setRuleId(migrationRuleRelationship.getRuleId());
    migrationSpeedRelationShipThrift
        .setRelationshipId(migrationRuleRelationship.getRelationshipId());
    migrationSpeedRelationShipThrift.setStoragePoolId(migrationRuleRelationship.getStoragePoolId());
    migrationSpeedRelationShipThrift.setMigrationRuleStatusBindingPools(
        MigrationRuleStatusBindingPoolsThrift
            .findByValue(migrationRuleRelationship.getStatus().getValue()));
    return migrationSpeedRelationShipThrift;
  }

  public static SegmentUnitType buildSegmentUnitTypeFromThrift(
      SegmentUnitTypeThrift segmentUnitTypeThrift) {
    SegmentUnitType segmentUnitType = null;
    if (segmentUnitTypeThrift == SegmentUnitTypeThrift.Arbiter) {
      segmentUnitType = SegmentUnitType.Arbiter;
    } else if (segmentUnitTypeThrift == SegmentUnitTypeThrift.Normal) {
      segmentUnitType = SegmentUnitType.Normal;
    } else if (segmentUnitTypeThrift == SegmentUnitTypeThrift.Flexible) {
      segmentUnitType = SegmentUnitType.Flexible;
    }
    return segmentUnitType;
  }

  public static List<InstanceIdAndEndPointThrift> buildInstanceIdAndEndPointThriftList(
      List<InstanceMetadataThrift> instanceMetadataThrifts) {
    List<InstanceIdAndEndPointThrift> instanceIdAndEndPointThrifts = new ArrayList<>();
    if (instanceMetadataThrifts == null || instanceMetadataThrifts.isEmpty()) {
      return instanceIdAndEndPointThrifts;
    }
    for (InstanceMetadataThrift instanceMetadataThrift : instanceMetadataThrifts) {
      InstanceIdAndEndPointThrift instanceIdAndEndPointThrift = new InstanceIdAndEndPointThrift();
      instanceIdAndEndPointThrift.setInstanceId(instanceMetadataThrift.getInstanceId());
      instanceIdAndEndPointThrift.setEndPoint(instanceMetadataThrift.getEndpoint());
      instanceIdAndEndPointThrift.setGroupId(instanceMetadataThrift.getGroup().getGroupId());
      instanceIdAndEndPointThrifts.add(instanceIdAndEndPointThrift);
    }
    return instanceIdAndEndPointThrifts;
  }

  public static VolumeSourceThrift buildThriftVolumeSource(
      VolumeMetadata.VolumeSourceType volumeSourceType) {
    VolumeSourceThrift volumeSourceThrift = null;
    if (volumeSourceType == VolumeMetadata.VolumeSourceType.CREATE_VOLUME) {
      volumeSourceThrift = CREATE;
    }
    return volumeSourceThrift;
  }

  public static VolumeMetadata.VolumeSourceType buildVolumeSourceTypeFrom(String requestType) {
    VolumeMetadata.VolumeSourceType volumeSourceType = null;
    if (requestType != null) {
      switch (requestType) {
        case "CREATE_VOLUME":
        case "EXTEND_VOLUME":
          volumeSourceType = VolumeMetadata.VolumeSourceType.CREATE_VOLUME;
          break;
        default:
          volumeSourceType = null;
      }
    }
    return volumeSourceType;
  }

  public static VolumeMetadata.VolumeSourceType buildVolumeSourceTypeFrom(
      VolumeSourceThrift volumeSourceThrift) {
    VolumeMetadata.VolumeSourceType volumeSourceType = null;
    if (volumeSourceThrift != null) {
      switch (volumeSourceThrift) {
        case CREATE:
          volumeSourceType = VolumeMetadata.VolumeSourceType.CREATE_VOLUME;
          break;
        default:
          volumeSourceType = null;
      }
    }
    return volumeSourceType;
  }

  public static VolumeInAction buildVolumeInActionFrom(VolumeInActionThrift volumeInActionThrift) {
    VolumeInAction volumeInAction = null;
    if (volumeInActionThrift != null) {
      switch (volumeInActionThrift) {
        case CREATING:
          volumeInAction = VolumeInAction.CREATING;
          break;
        case EXTENDING:
          volumeInAction = VolumeInAction.EXTENDING;
          break;
        case DELETING:
          volumeInAction = VolumeInAction.DELETING;
          break;
        case RECYCLING:
          volumeInAction = VolumeInAction.RECYCLING;
          break;
        case FIXING:
          volumeInAction = VolumeInAction.FIXING;
          break;
        case NULL:
          volumeInAction = VolumeInAction.NULL;
          break;
        default:
          volumeInAction = null;
      }
    }
    return volumeInAction;
  }

  public static VolumeInActionThrift buildThriftVolumeInAction(VolumeInAction volumeInAction) {
    VolumeInActionThrift volumeInActionThrift = null;
    if (volumeInAction.equals(VolumeInAction.CREATING)) {
      volumeInActionThrift = VolumeInActionThrift.CREATING;
    } else if (volumeInAction.equals(VolumeInAction.EXTENDING)) {
      volumeInActionThrift = VolumeInActionThrift.EXTENDING;
    } else if (volumeInAction.equals(VolumeInAction.DELETING)) {
      volumeInActionThrift = VolumeInActionThrift.DELETING;
    } else if (volumeInAction.equals(VolumeInAction.RECYCLING)) {
      volumeInActionThrift = VolumeInActionThrift.RECYCLING;
    } else if (volumeInAction.equals(VolumeInAction.FIXING)) {
      volumeInActionThrift = VolumeInActionThrift.FIXING;
    } else {
      volumeInActionThrift = VolumeInActionThrift.NULL;
    }
    return volumeInActionThrift;
  }

  public static ScsiDeviceStatusThrift buildScsiDeviceStatusType(String scsiDeviceStatus) {
    ScsiDeviceStatusThrift scsiDeviceStatusThrifts = null;
    if (scsiDeviceStatus.equals(ScsiDeviceStatusThrift.CREATING.name())) {
      scsiDeviceStatusThrifts = ScsiDeviceStatusThrift.CREATING;
    } else if (scsiDeviceStatus == ScsiDeviceStatusThrift.NORMAL.name()) {
      scsiDeviceStatusThrifts = ScsiDeviceStatusThrift.NORMAL;
    } else if (scsiDeviceStatus == ScsiDeviceStatusThrift.RECOVERY.name()) {
      scsiDeviceStatusThrifts = ScsiDeviceStatusThrift.RECOVERY;
    } else if (scsiDeviceStatus == ScsiDeviceStatusThrift.UMOUNT.name()) {
      scsiDeviceStatusThrifts = ScsiDeviceStatusThrift.UMOUNT;
    } else if (scsiDeviceStatus == ScsiDeviceStatusThrift.ERROR.name()) {
      scsiDeviceStatusThrifts = ScsiDeviceStatusThrift.ERROR;
    } else if (scsiDeviceStatus == ScsiDeviceStatusThrift.CONNECTEXCEPTIONRECOVERING.name()) {
      scsiDeviceStatusThrifts = ScsiDeviceStatusThrift.CONNECTEXCEPTIONRECOVERING;
    } else if (scsiDeviceStatus == ScsiDeviceStatusThrift.LAUNCHING.name()) {
      scsiDeviceStatusThrifts = ScsiDeviceStatusThrift.LAUNCHING;
    } else if (scsiDeviceStatus == ScsiDeviceStatusThrift.UNKNOWN.name()) {
      scsiDeviceStatusThrifts = ScsiDeviceStatusThrift.UNKNOWN;
    } else if (scsiDeviceStatus == ScsiDeviceStatusThrift.REMOVING.name()) {
      scsiDeviceStatusThrifts = ScsiDeviceStatusThrift.REMOVING;
    } else if (scsiDeviceStatus == ScsiDeviceStatusThrift.CONNECTING.name()) {
      scsiDeviceStatusThrifts = ScsiDeviceStatusThrift.CONNECTING;
    } else if (scsiDeviceStatus == ScsiDeviceStatusThrift.DISCONNECTING.name()) {
      scsiDeviceStatusThrifts = ScsiDeviceStatusThrift.DISCONNECTING;
    }
    return scsiDeviceStatusThrifts;
  }

  public static AccessPermissionTypeThrift buildThriftAccessPermissionType(
      AccessPermissionType accessPermissionType) {
    AccessPermissionTypeThrift accessPermissionTypeThrift = null;
    if (accessPermissionType == AccessPermissionType.READ) {
      accessPermissionTypeThrift = AccessPermissionTypeThrift.READ;
    } else if (accessPermissionType == AccessPermissionType.WRITE) {
      accessPermissionTypeThrift = AccessPermissionTypeThrift.WRITE;
    } else if (accessPermissionType == AccessPermissionType.READWRITE) {
      accessPermissionTypeThrift = AccessPermissionTypeThrift.READWRITE;
    }
    return accessPermissionTypeThrift;
  }

  public static AccessPermissionType buildAccessPermissionType(
      AccessPermissionTypeThrift accessPermissionTypeThrift) {
    AccessPermissionType accessPermissionType = null;
    if (accessPermissionTypeThrift == AccessPermissionTypeThrift.READ) {
      accessPermissionType = AccessPermissionType.READ;
    } else if (accessPermissionTypeThrift == AccessPermissionTypeThrift.WRITE) {
      accessPermissionType = AccessPermissionType.WRITE;
    } else if (accessPermissionTypeThrift == AccessPermissionTypeThrift.READWRITE) {
      accessPermissionType = AccessPermissionType.READWRITE;
    }
    return accessPermissionType;
  }

  public static VolumeMetadata.ReadWriteType buildReadWriteTypeFrom(
      ReadWriteTypeThrift readWriteTypeThrift) {
    VolumeMetadata.ReadWriteType readWriteType = null;
    if (null != readWriteTypeThrift) {
      switch (readWriteTypeThrift) {
        case READONLY:
          readWriteType = VolumeMetadata.ReadWriteType.READONLY;
          break;
        case READWRITE:
          readWriteType = VolumeMetadata.ReadWriteType.READWRITE;
          break;
        default:
          readWriteType = null;
      }
    }
    return readWriteType;
  }

  public static ReadWriteTypeThrift buildThriftReadWriteType(
      VolumeMetadata.ReadWriteType readWriteType) {
    ReadWriteTypeThrift readWriteTypeThrift = null;
    if (readWriteType == VolumeMetadata.ReadWriteType.READONLY) {
      readWriteTypeThrift = ReadWriteTypeThrift.READONLY;
    } else if (readWriteType == VolumeMetadata.ReadWriteType.READWRITE) {
      readWriteTypeThrift = ReadWriteTypeThrift.READWRITE;
    }
    return readWriteTypeThrift;
  }

  public static AlertMessageThrift buildAlertMessageThriftFrom(AlertMessage alertMessage) {
    AlertMessageThrift alertMessageThrift = new AlertMessageThrift();
    alertMessageThrift.setId(alertMessage.getId());
    alertMessageThrift.setAlertAcknowledge(alertMessage.isAlertAcknowledge());
    alertMessageThrift.setAlertClear(alertMessage.isAlertClear());
    alertMessageThrift.setClearTime(alertMessage.getClearTime());
    alertMessageThrift.setAlertAcknowledgeTime(alertMessage.getAlertAcknowledgeTime());
    alertMessageThrift.setAlertDescription(alertMessage.getAlertDescription());
    alertMessageThrift.setAlertFrequency(alertMessage.getAlertFrequency());
    alertMessageThrift.setAlertLevel(alertMessage.getAlertLevel());
    alertMessageThrift.setAlertRuleName(alertMessage.getAlertRuleName());
    alertMessageThrift.setAlertType(alertMessage.getAlertType());
    alertMessageThrift.setFirstAlertTime(alertMessage.getFirstAlertTime());
    alertMessageThrift.setLastAlertTime(alertMessage.getLastAlertTime());
    alertMessageThrift.setSourceId(alertMessage.getSourceId());
    alertMessageThrift.setSourceName(alertMessage.getSourceName());
    return alertMessageThrift;
  }

  public static AlertMessageDetailThrift buildAlertMessageDetailThriftFrom(
      AlertMessage alertMessage) {
    AlertMessageDetailThrift alertMessageDetail = new AlertMessageDetailThrift();
    alertMessageDetail.setAlertMessageThrift(buildAlertMessageThriftFrom(alertMessage));
    alertMessageDetail.setIp(alertMessage.getIp());
    alertMessageDetail.setHostname(alertMessage.getHostname());
    alertMessageDetail.setAlertItem(alertMessage.getAlertItem());
    alertMessageDetail.setSerialNum(alertMessage.getSerialNum());
    alertMessageDetail.setSlotNo(alertMessage.getSlotNo());
    alertMessageDetail.setServerNodeRackNo(alertMessage.getServerNodeRackNo());
    alertMessageDetail.setServerNodeChildFramNo(alertMessage.getServerNodeChildFramNo());
    alertMessageDetail.setServerNodeSlotNo(alertMessage.getServerNodeSlotNo());
    alertMessageDetail.setLastActualValue(alertMessage.getLastActualValue());
    return alertMessageDetail;
  }

  public static AlertMessage buildAlertMessageFrom(AlertMessageDetailThrift alertMessageDetail) {
    AlertMessage alertMessage = buildAlertMessageFrom(alertMessageDetail.getAlertMessageThrift());
    alertMessage.setIp(alertMessageDetail.getIp());
    alertMessage.setHostname(alertMessageDetail.getHostname());
    alertMessage.setAlertItem(alertMessageDetail.getAlertItem());
    alertMessage.setSerialNum(alertMessageDetail.getSerialNum());
    alertMessage.setSlotNo(alertMessageDetail.getSlotNo());
    return alertMessage;
  }

  public static AlertMessage buildAlertMessageFrom(AlertMessageThrift alertMessageThrift) {
    AlertMessage alertMessage = new AlertMessage();
    alertMessage.setId(alertMessageThrift.getId());
    alertMessage.setAlertAcknowledge(alertMessageThrift.isAlertAcknowledge());
    alertMessage.setAlertClear(alertMessageThrift.isAlertClear());
    alertMessage.setClearTime(alertMessageThrift.getClearTime());
    alertMessage.setAlertAcknowledgeTime(alertMessageThrift.getAlertAcknowledgeTime());
    alertMessage.setAlertDescription(alertMessageThrift.getAlertDescription());
    alertMessage.setAlertFrequency(alertMessageThrift.getAlertFrequency());
    alertMessage.setAlertLevel(alertMessageThrift.getAlertLevel());
    alertMessage.setAlertRuleName(alertMessageThrift.getAlertRuleName());
    alertMessage.setAlertType(alertMessageThrift.getAlertType());
    alertMessage.setFirstAlertTime(alertMessageThrift.getFirstAlertTime());
    alertMessage.setLastAlertTime(alertMessageThrift.getLastAlertTime());
    alertMessage.setSourceId(alertMessageThrift.getSourceId());
    alertMessage.setSourceName(alertMessageThrift.getSourceName());
    return alertMessage;
  }

  public static PerformanceTask buildPerformanceTaskFrom(
      PerformanceTaskThrift performanceTaskThrift) {
    PerformanceTask performanceTask = new PerformanceTask();
    performanceTask.setId(performanceTaskThrift.getId());
    performanceTask.setCounterKey(performanceTaskThrift.getCounterKey());
    performanceTask.setSourceId(performanceTaskThrift.getSourceId());
    performanceTask.setSourceName(performanceTaskThrift.getSourceName());
    performanceTask.setStartTime(performanceTaskThrift.getStartTime());
    performanceTask.setStatus(performanceTaskThrift.isStatus());
    return performanceTask;
  }

  public static PerformanceTaskThrift buildPerformanceTaskThriftFrom(
      PerformanceTask performanceTask) {
    PerformanceTaskThrift performanceTaskThrift = new PerformanceTaskThrift();
    performanceTaskThrift.setId(performanceTask.getId());
    performanceTaskThrift.setCounterKey(performanceTask.getCounterKey());
    performanceTaskThrift.setSourceId(performanceTask.getSourceId());
    performanceTaskThrift.setSourceName(performanceTask.getSourceName());
    performanceTaskThrift.setStartTime(performanceTask.getStartTime());
    performanceTaskThrift.setStatus(performanceTask.isStatus());
    return performanceTaskThrift;
  }

  public static EventLogCompressed buildEventLogCompressedFrom(
      EventLogCompressedThrift eventLogCompressedThrift) {
    EventLogCompressed eventLogCompressed = new EventLogCompressed();
    eventLogCompressed.setId(eventLogCompressedThrift.getId());
    eventLogCompressed.setOperation(eventLogCompressedThrift.getOperation());
    eventLogCompressed.setCounterKey(eventLogCompressedThrift.getCounterKey());
    eventLogCompressed.setSourceId(eventLogCompressedThrift.getSourceId());
    eventLogCompressed.setCounterTotal(new AtomicLong(eventLogCompressedThrift.getCounterTotal()));
    eventLogCompressed.setStartTime(new AtomicLong(eventLogCompressedThrift.getStartTime()));
    eventLogCompressed.setEndTime(new AtomicLong(eventLogCompressedThrift.getEndTime()));
    eventLogCompressed.setFrequency(new AtomicInteger(eventLogCompressedThrift.getFrequency()));
    return eventLogCompressed;
  }

  public static EventLogCompressedThrift buildEventLogCompressedThriftFrom(
      EventLogCompressed eventLogCompressed) {
    EventLogCompressedThrift eventLogCompressedThrift = new EventLogCompressedThrift();
    eventLogCompressedThrift.setId(eventLogCompressed.getId());
    eventLogCompressedThrift.setOperation(eventLogCompressed.getOperation());
    eventLogCompressedThrift.setCounterKey(eventLogCompressed.getCounterKey());
    eventLogCompressedThrift.setSourceId(eventLogCompressed.getSourceId());
    eventLogCompressedThrift.setCounterTotal(eventLogCompressed.getCounterTotal().get());
    eventLogCompressedThrift.setStartTime(eventLogCompressed.getStartTime().get());
    eventLogCompressedThrift.setEndTime(eventLogCompressed.getEndTime().get());
    eventLogCompressedThrift.setFrequency(eventLogCompressed.getFrequency().get());
    return eventLogCompressedThrift;
  }

  public static EventLogCompressedThrift buildEventLogCompressedThriftFrom(
      PerformanceMessageHistory performanceMessageHistory) {
    EventLogCompressedThrift eventLogCompressedThrift = new EventLogCompressedThrift();
    eventLogCompressedThrift.setId(performanceMessageHistory.getId());
    eventLogCompressedThrift.setSourceId(performanceMessageHistory.getSourceId());
    eventLogCompressedThrift.setOperation(performanceMessageHistory.getOperation());
    eventLogCompressedThrift.setCounterKey(performanceMessageHistory.getCounterKey());
    eventLogCompressedThrift.setCounterTotal(performanceMessageHistory.getCounterTotal());
    eventLogCompressedThrift.setFrequency(performanceMessageHistory.getFrequency());
    eventLogCompressedThrift.setStartTime(performanceMessageHistory.getStartTime().getTime());
    eventLogCompressedThrift.setEndTime(performanceMessageHistory.getEndTime().getTime());
    return eventLogCompressedThrift;
  }

  public static AlertTemplate buildAlertTemplateFrom(AlertTemplateThrift alertTemplateThrift) {
    AlertTemplate alertTemplate = new AlertTemplate();

    alertTemplate.setId(alertTemplateThrift.getId());
    alertTemplate.setName(alertTemplateThrift.getName());
    alertTemplate.setSourceId(alertTemplateThrift.getSourceId());

    Map<String, AlertRule> alertRuleMap = new HashMap<>();
    Map<String, AlertRuleThrift> alertRuleThriftMap = alertTemplateThrift.getAlertRuleMap();
    for (Map.Entry<String, AlertRuleThrift> entry : alertRuleThriftMap.entrySet()) {
      String key = entry.getKey();
      AlertRuleThrift value = entry.getValue();
      AlertRule alertRule = buildAlertRuleFrom(value);
      alertRule.setAlertTemplate(alertTemplate);
      alertRuleMap.put(key, alertRule);
    }
    alertTemplate.setAlertRuleMap(alertRuleMap);
    return alertTemplate;
  }

  public static AlertTemplateThrift buildAlertTemplateThriftFrom(AlertTemplate alertTemplate) {
    AlertTemplateThrift alertTemplateThrift = new AlertTemplateThrift();

    alertTemplateThrift.setId(alertTemplate.getId());
    alertTemplateThrift.setName(alertTemplate.getName());
    alertTemplateThrift.setSourceId(alertTemplate.getSourceId());

    Map<String, AlertRuleThrift> alertRuleThriftMap = new HashMap<>();
    Map<String, AlertRule> alertRuleMap = alertTemplate.getAlertRuleMap();
    for (Map.Entry<String, AlertRule> entry : alertRuleMap.entrySet()) {
      String key = entry.getKey();
      AlertRule value = entry.getValue();
      alertRuleThriftMap.put(key, buildAlertRuleThriftFrom(value));
    }

    alertTemplateThrift.setAlertRuleMap(alertRuleThriftMap);

    return alertTemplateThrift;
  }

  public static AlertRuleThrift buildAlertRuleThriftFrom(AlertRule alertRule) {
    AlertRuleThrift alertRuleThrift = new AlertRuleThrift();
    alertRuleThrift.setId(alertRule.getId());
    alertRuleThrift.setName(alertRule.getName());
    alertRuleThrift.setCounterKey(alertRule.getCounterKey());
    alertRuleThrift.setDescription(alertRule.getDescription());
    alertRuleThrift.setAlertLevelOne(alertRule.getAlertLevelOne());
    alertRuleThrift.setAlertLevelTwo(alertRule.getAlertLevelTwo());
    alertRuleThrift.setAlertLevelOneThreshold(alertRule.getAlertLevelOneThreshold());
    alertRuleThrift.setAlertLevelTwoThreshold(alertRule.getAlertLevelTwoThreshold());
    alertRuleThrift.setRelationOperator(alertRule.getRelationOperator());
    alertRuleThrift.setContinuousOccurTimes(alertRule.getContinuousOccurTimes());
    alertRuleThrift.setRepeatAlert(alertRule.isRepeatAlert());
    alertRuleThrift.setLeftId(alertRule.getLeftId());
    alertRuleThrift.setRightId(alertRule.getRightId());
    alertRuleThrift.setParentId(alertRule.getParentId());
    alertRuleThrift.setLogicOperator(alertRule.getLogicOperator());
    alertRuleThrift.setAlertRecoveryThreshold(alertRule.getAlertRecoveryThreshold());
    alertRuleThrift.setAlertRecoveryRelationOperator(alertRule.getAlertRecoveryRelationOperator());
    alertRuleThrift
        .setAlertRecoveryEventContinuousOccurTimes(
            alertRule.getAlertRecoveryEventContinuousOccurTimes());
    alertRuleThrift.setEnable(alertRule.isEnable());
    alertRuleThrift.setAlertTemplateId(alertRule.getAlertTemplate().getId());
    return alertRuleThrift;
  }

  public static AlertRule buildAlertRuleFrom(AlertRuleThrift alertRuleThrift) {
    AlertRule alertRule = new AlertRule();
    alertRule.setId(alertRuleThrift.getId());
    alertRule.setName(alertRuleThrift.getName());
    alertRule.setCounterKey(alertRuleThrift.getCounterKey());
    alertRule.setDescription(alertRuleThrift.getDescription());
    alertRule.setAlertLevelOne(alertRuleThrift.getAlertLevelOne());
    alertRule.setAlertLevelTwo(alertRuleThrift.getAlertLevelTwo());
    alertRule.setAlertLevelOneThreshold(alertRuleThrift.getAlertLevelOneThreshold());
    alertRule.setAlertLevelTwoThreshold(alertRuleThrift.getAlertLevelTwoThreshold());
    alertRule.setRelationOperator(alertRuleThrift.getRelationOperator());
    alertRule.setContinuousOccurTimes(alertRuleThrift.getContinuousOccurTimes());
    alertRule.setRepeatAlert(alertRuleThrift.isRepeatAlert());
    alertRule.setLeftId(alertRuleThrift.getLeftId());
    alertRule.setRightId(alertRuleThrift.getRightId());
    alertRule.setParentId(alertRuleThrift.getParentId());
    alertRule.setLogicOperator(alertRuleThrift.getLogicOperator());
    alertRule.setAlertRecoveryThreshold(alertRuleThrift.getAlertRecoveryThreshold());
    alertRule.setAlertRecoveryRelationOperator(alertRuleThrift.getAlertRecoveryRelationOperator());
    alertRule.setAlertRecoveryEventContinuousOccurTimes(
        alertRuleThrift.getAlertRecoveryEventContinuousOccurTimes());
    alertRule.setEnable(alertRuleThrift.isEnable());
    //alertRule.setAlertTemplate(alertRuleThrift.getAlertTemplateId());
    return alertRule;
  }

  public static ApiToAuthorizeThrift buildApiToAuthorizeThrift(ApiToAuthorize api) {
    Validate.notNull(api);
    return new ApiToAuthorizeThrift(api.getApiName(), api.getCategory(), api.getChineseText(),
        api.getEnglishText());
  }

  public static ApiToAuthorize buildApiToAuthorizeFrom(ApiToAuthorizeThrift apiToAuthorizeThrift) {
    return new ApiToAuthorize(apiToAuthorizeThrift.getApiName(), apiToAuthorizeThrift.getCategory(),
        apiToAuthorizeThrift.getChineseText(), apiToAuthorizeThrift.getEnglishText());
  }

  public static RoleThrift buildRoleThrift(Role role) {
    Validate.notNull(role);
    RoleThrift roleThrift = new RoleThrift();
    roleThrift.setId(role.getId());
    roleThrift.setName(role.getName());
    roleThrift.setDescription(role.getDescription());
    Set<ApiToAuthorizeThrift> permissionThrifts = new HashSet<>();
    roleThrift.setPermissions(permissionThrifts);
    for (ApiToAuthorize permission : role.getPermissions()) {
      permissionThrifts.add(buildApiToAuthorizeThrift(permission));
    }
    roleThrift.setBuiltIn(role.isBuiltIn());
    roleThrift.setSuperAdmin(role.isSuperAdmin());
    return roleThrift;
  }

  public static Role buildRoleFrom(RoleThrift roleThrift) {
    Role role = new Role(roleThrift.getId(), roleThrift.getName());
    role.setDescription(roleThrift.getDescription());
    Set<ApiToAuthorize> permissions = new HashSet<>();
    role.setPermissions(permissions);
    for (ApiToAuthorizeThrift api : roleThrift.getPermissions()) {
      permissions.add(buildApiToAuthorizeFrom(api));
    }
    role.setBuiltIn(roleThrift.isBuiltIn());
    role.setSuperAdmin(roleThrift.isSuperAdmin());
    return role;
  }

  public static ResourceThrift buildResourceThrift(PyResource resource) {
    return new ResourceThrift(resource.getResourceId(), resource.getResourceName(),
        resource.getResourceType());
  }

  public static PyResource buildResourceFrom(ResourceThrift resourceThrift) {
    return new PyResource(resourceThrift.getResourceId(), resourceThrift.getResourceName(),
        resourceThrift.getResourceType());
  }

  public static DiskSmartInfoThrift buildDiskSmartInfoThriftFrom(DiskSmartInfo smartInfo) {
    DiskSmartInfoThrift smartInfoThrift = new DiskSmartInfoThrift();
    smartInfoThrift.setId(smartInfo.getId());
    smartInfoThrift.setAttributeNameEn(smartInfo.getAttributeNameEn());
    smartInfoThrift.setAttributeNameCn(smartInfo.getAttributeNameCn());
    smartInfoThrift.setFlag(smartInfo.getFlag());
    smartInfoThrift.setValue(smartInfo.getValue());
    smartInfoThrift.setWorst(smartInfo.getWorst());
    smartInfoThrift.setThresh(smartInfo.getThresh());
    smartInfoThrift.setType(smartInfo.getType());
    smartInfoThrift.setUpdated(smartInfo.getUpdated());
    smartInfoThrift.setWhenFailed(smartInfo.getWhenFailed());
    smartInfoThrift.setRawValue(smartInfo.getRawValue());
    return smartInfoThrift;
  }

  public static DiskSmartInfo buildDiskSmartInfoFrom(DiskSmartInfoThrift smartInfoThrift) {
    DiskSmartInfo smartInfo = new DiskSmartInfo();
    smartInfo.setId(smartInfoThrift.getId());
    smartInfo.setAttributeNameEn(smartInfoThrift.getAttributeNameEn());
    smartInfo.setAttributeNameCn(smartInfoThrift.getAttributeNameCn());
    smartInfo.setFlag(smartInfoThrift.getFlag());
    smartInfo.setValue(smartInfoThrift.getValue());
    smartInfo.setWorst(smartInfoThrift.getWorst());
    smartInfo.setThresh(smartInfoThrift.getThresh());
    smartInfo.setType(smartInfoThrift.getType());
    smartInfo.setUpdated(smartInfoThrift.getUpdated());
    smartInfo.setWhenFailed(smartInfoThrift.getWhenFailed());
    smartInfo.setRawValue(smartInfoThrift.getRawValue());
    return smartInfo;
  }

  public static SensorInfo buildSensorInfoFrom(SensorInfoThrift sensorInfoThrift) {
    SensorInfo sensorInfo = new SensorInfo();
    sensorInfo.setName(sensorInfoThrift.getName());
    sensorInfo.setValue(sensorInfoThrift.getValue());
    sensorInfo.setStatus(sensorInfoThrift.getStatus());
    return sensorInfo;
  }

  public static SensorInfoThrift buildSensorInfoThriftFrom(SensorInfo sensorInfo) {
    SensorInfoThrift sensorInfoThrift = new SensorInfoThrift();
    sensorInfoThrift.setName(sensorInfo.getName());
    sensorInfoThrift.setValue(sensorInfo.getValue());
    sensorInfoThrift.setStatus(sensorInfo.getStatus());
    return sensorInfoThrift;
  }

  public static HardDiskInfoThrift buildThriftDiskInfoFrom(DiskInfo diskInfo) {
    HardDiskInfoThrift diskInfoThrift = new HardDiskInfoThrift();
    diskInfoThrift.setSn(diskInfo.getSn());
    diskInfoThrift.setRate(diskInfo.getRate());
    diskInfoThrift.setModel(diskInfo.getModel());
    diskInfoThrift.setName(diskInfo.getName());
    diskInfoThrift.setSsdOrHdd(diskInfo.getSsdOrHdd());
    diskInfoThrift.setVendor(diskInfo.getVendor());
    diskInfoThrift.setSize(diskInfo.getSize());
    diskInfoThrift.setWwn(diskInfo.getWwn());
    diskInfoThrift.setControllerId(diskInfo.getControllerId());
    diskInfoThrift.setSlotNumber(diskInfo.getSlotNumber());
    diskInfoThrift.setEnclosureId(diskInfo.getEnclosureId());
    diskInfoThrift.setCardType(diskInfo.getCardType());
    diskInfoThrift.setSwith(diskInfo.getSwith());
    diskInfoThrift.setSerialNumber(diskInfo.getSerialNumber());
    diskInfoThrift.setId(diskInfo.getId());

    List<DiskSmartInfoThrift> diskSmartInfoThriftList = new LinkedList<>();
    diskInfoThrift.setSmartInfo(diskSmartInfoThriftList);

    return diskInfoThrift;
  }

  /**
   * build disk info without smart info.
   */
  public static DiskInfo buildDiskInfoFrom(HardDiskInfoThrift diskInfoThrift) {
    DiskInfo diskInfo = new DiskInfo();
    diskInfo.setSn(diskInfoThrift.getSn());
    diskInfo.setRate(diskInfoThrift.getRate());
    diskInfo.setModel(diskInfoThrift.getModel());
    diskInfo.setName(diskInfoThrift.getName());
    diskInfo.setSsdOrHdd(diskInfoThrift.getSsdOrHdd());
    diskInfo.setVendor(diskInfoThrift.getVendor());
    diskInfo.setSize(diskInfoThrift.getSize());
    diskInfo.setWwn(diskInfoThrift.getWwn());
    diskInfo.setControllerId(diskInfoThrift.getControllerId());
    diskInfo.setSlotNumber(diskInfoThrift.getSlotNumber());
    diskInfo.setEnclosureId(diskInfoThrift.getEnclosureId());
    diskInfo.setCardType(diskInfoThrift.getCardType());
    diskInfo.setSwith(diskInfoThrift.getSwith());
    diskInfo.setSerialNumber(diskInfoThrift.getSerialNumber());
    diskInfo.setId(diskInfoThrift.getId());

    return diskInfo;
  }

  public static ServerNodeThrift buildThriftServerNodeFrom(ServerNode serverNode) {
    Validate.notNull(serverNode);
    ServerNodeThrift serverNodeThrift = new ServerNodeThrift();
    serverNodeThrift.setServerId(serverNode.getId());
    serverNodeThrift.setHostName(serverNode.getHostName());
    serverNodeThrift.setCpuInfo(serverNode.getCpuInfo());
    serverNodeThrift.setDiskInfo(serverNode.getDiskInfo());
    serverNodeThrift.setMemoryInfo(serverNode.getMemoryInfo());
    serverNodeThrift.setModelInfo(serverNode.getModelInfo());
    serverNodeThrift.setNetworkCardInfo(serverNode.getNetworkCardInfo());
    serverNodeThrift.setNetworkCardInfoName(serverNode.getNetworkCardInfoName());
    serverNodeThrift.setGatewayIp(serverNode.getGatewayIp());
    serverNodeThrift.setManageIp(serverNode.getManageIp());
    serverNodeThrift.setStoreIp(serverNode.getStoreIp());
    serverNodeThrift.setRackNo(serverNode.getRackNo());
    serverNodeThrift.setSlotNo(serverNode.getSlotNo());
    serverNodeThrift.setStatus(serverNode.getStatus());
    serverNodeThrift.setChildFramNo(serverNode.getChildFramNo());
    Set<SensorInfo> sensorInfos = serverNode.getSensorInfoList();
    Set<SensorInfoThrift> sensorInfoThrifts = new HashSet<>();
    for (SensorInfo sensorInfo : sensorInfos) {
      SensorInfoThrift sensorInfoThrift = buildSensorInfoThriftFrom(sensorInfo);
      sensorInfoThrifts.add(sensorInfoThrift);
    }
    serverNodeThrift.setSensorInfos(sensorInfoThrifts);
    Set<HardDiskInfoThrift> diskInfoThrifts = new HashSet<>();
    for (DiskInfo diskInfo : serverNode.getDiskInfoSet()) {
      HardDiskInfoThrift diskInfoThrift = buildThriftDiskInfoFrom(diskInfo);
      diskInfoThrifts.add(diskInfoThrift);
    }
    serverNodeThrift.setDiskInfoSet(diskInfoThrifts);
    serverNodeThrift
        .setDatanodeStatus(convertThriftDatanodeStatus(serverNode.getDatanodeStatus()));

    return serverNodeThrift;
  }

  public static ServerNode buildServerNodeFrom(ServerNodeThrift serverNodeThrift) {
    Validate.notNull(serverNodeThrift);
    ServerNode serverNode = new ServerNode();
    serverNode.setId(serverNodeThrift.getServerId());
    serverNode.setCpuInfo(serverNodeThrift.getCpuInfo());
    serverNode.setDiskInfo(serverNodeThrift.getDiskInfo());
    serverNode.setMemoryInfo(serverNodeThrift.getMemoryInfo());
    serverNode.setHostName(serverNodeThrift.getHostName());
    serverNode.setModelInfo(serverNodeThrift.getModelInfo());
    serverNode.setNetworkCardInfo(serverNodeThrift.getNetworkCardInfo());
    serverNode.setNetworkCardInfo(serverNodeThrift.getNetworkCardInfo());
    serverNode.setGatewayIp(serverNodeThrift.getGatewayIp());
    serverNode.setManageIp(serverNodeThrift.getManageIp());
    serverNode.setStoreIp(serverNodeThrift.getStoreIp());
    serverNode.setRackNo(serverNodeThrift.getRackNo());
    serverNode.setSlotNo(serverNodeThrift.getSlotNo());
    serverNode.setStatus(serverNodeThrift.getStatus());
    serverNode.setChildFramNo(serverNodeThrift.getChildFramNo());
    Set<DiskInfo> diskInfoSet = new HashSet<>();
    for (HardDiskInfoThrift diskInfoThrift : serverNodeThrift.getDiskInfoSet()) {
      DiskInfo diskInfo = buildDiskInfoFrom(diskInfoThrift);
      diskInfoSet.add(diskInfo);
    }
    serverNode.setDiskInfoSet(diskInfoSet);
    serverNode
        .setDatanodeStatus(convertFromDatanodeStatusThrift(serverNodeThrift.getDatanodeStatus()));
    Set<SensorInfoThrift> sensorInfoThrifts = new HashSet<>();
    serverNodeThrift.setSensorInfos(sensorInfoThrifts);
    return serverNode;
  }

  public static PerformanceSearchTemplateThrift buildPerformanceSearchTemplateThriftFrom(
      PerformanceSearchTemplate performanceSearchTemplate) {
    PerformanceSearchTemplateThrift performanceSearchTemplateThrift =
        new PerformanceSearchTemplateThrift();
    performanceSearchTemplateThrift.setId(performanceSearchTemplate.getId());
    performanceSearchTemplateThrift.setName(performanceSearchTemplate.getName());
    performanceSearchTemplateThrift
        .setStartTime(performanceSearchTemplate.getStartTime().getTime());
    performanceSearchTemplateThrift.setEndTime(performanceSearchTemplate.getEndTime().getTime());
    performanceSearchTemplateThrift.setTimeUnit(performanceSearchTemplate.getTimeUnit());
    performanceSearchTemplateThrift
        .setCounterKeyJson(performanceSearchTemplate.getCounterKeyJson());
    performanceSearchTemplateThrift.setSourcesJson(performanceSearchTemplate.getSourcesJson());
    performanceSearchTemplateThrift.setAccountId(performanceSearchTemplate.getAccountId());
    performanceSearchTemplateThrift.setObjectType(performanceSearchTemplate.getObjectType());
    return performanceSearchTemplateThrift;
  }

  public static PerformanceSearchTemplate buildPerformanceSearchTemplateFrom(
      PerformanceSearchTemplateThrift performanceSearchTemplateThrift) {
    PerformanceSearchTemplate performanceSearchTemplate = new PerformanceSearchTemplate();
    performanceSearchTemplate.setId(performanceSearchTemplateThrift.getId());
    performanceSearchTemplate.setName(performanceSearchTemplateThrift.getName());
    performanceSearchTemplate
        .setStartTime(new Date(performanceSearchTemplateThrift.getStartTime()));
    performanceSearchTemplate.setEndTime(new Date(performanceSearchTemplateThrift.getEndTime()));
    performanceSearchTemplate.setTimeUnit(performanceSearchTemplateThrift.getTimeUnit());
    performanceSearchTemplate
        .setCounterKeyJson(performanceSearchTemplateThrift.getCounterKeyJson());
    performanceSearchTemplate.setSourcesJson(performanceSearchTemplateThrift.getSourcesJson());
    performanceSearchTemplate.setAccountId(performanceSearchTemplateThrift.getAccountId());
    performanceSearchTemplate.setObjectType(performanceSearchTemplateThrift.getObjectType());
    return performanceSearchTemplate;
  }

  public static DriverKey buildDriverKeyFrom(DriverKeyThrift driverKeyThrift) {
    Validate.notNull(driverKeyThrift.getDriverType());
    DriverKey driverKey = new DriverKey(driverKeyThrift.getDriverContainerId(),
        driverKeyThrift.getVolumeId(),
        driverKeyThrift.getSnapshotId(),
        DriverType.valueOf(driverKeyThrift.getDriverType().name()));
    return driverKey;
  }

  public static DriverKeyThrift buildThriftDriverKeyFrom(DriverKey driverKey) {
    DriverKeyThrift driverKeyThrift = new DriverKeyThrift();
    driverKeyThrift.setDriverContainerId(driverKey.getDriverContainerId());
    driverKeyThrift.setVolumeId(driverKey.getVolumeId());
    driverKeyThrift.setDriverType(DriverTypeThrift.valueOf(driverKey.getDriverType().name()));

    driverKeyThrift.setSnapshotId(driverKey.getSnapshotId());
    return driverKeyThrift;
  }

  public static boolean judgeDynamicIoLimitationTimeInterleaving(IoLimitation updateIoLimitation,
      List<IoLimitation> existIoLimitations) {
    if (updateIoLimitation == null || existIoLimitations == null || existIoLimitations.isEmpty()) {
      return false;
    }

    if (updateIoLimitation.getLimitType() != IoLimitation.LimitType.Dynamic) {
      return false;
    }

    return false;
  }

  public static boolean judgeDynamicIoLimitationTimeInterleaving(IoLimitation updateIoLimitation) {
    if (updateIoLimitation == null) {
      return false;
    }
    if (updateIoLimitation.getLimitType() != IoLimitation.LimitType.Dynamic) {
      return false;
    }

    List<IoLimitationEntry> entries = updateIoLimitation.getEntries();
    String name = updateIoLimitation.getName();
    if (Constants.SPECIAL_IO_LIMIT_NAME_FOR_CSI.equals(name)) {
      logger.warn("when judgeDynamicIoLimitationTimeInterleaving for name:{}, is can do it", name);
      return false;
    }

    for (int i = 0; i < entries.size(); i++) {
      IoLimitationEntry entry1 = entries.get(i);
      if (entry1.getStartTimeWithLocalFormat().isAfter(entry1.getEndTimeWithLocalFormat())) {
        return true;
      }
      for (int j = i + 1; j < entries.size(); j++) {
        IoLimitationEntry entry2 = entries.get(j);
        Validate.notNull(entry1.getStartTimeWithLocalFormat());
        Validate.notNull(entry1.getEndTimeWithLocalFormat());
        Validate.notNull(entry2.getStartTimeWithLocalFormat());
        Validate.notNull(entry2.getEndTimeWithLocalFormat());

        if (entry2.getStartTimeWithLocalFormat().isAfter(entry1.getEndTimeWithLocalFormat())
            || entry2
            .getEndTimeWithLocalFormat().isBefore(entry1.getStartTimeWithLocalFormat())) {
          logger.debug("");
        } else {
          return true;
        }
      }
    }
    return false;
  }

  public static OperationThrift buildThriftOperationFrom(Operation operation) {
    logger.debug("access build operation thrift from operation");
    logger.debug("begin build operation-thrift by operation {}", operation);
    Validate.notNull(operation);
    Validate.notNull(operation.getOperationId());

    //        Validate.notNull(operation.getTargetId());
    Validate.notNull(operation.getStartTime());
    Validate.notNull(operation.getEndTime());
    Validate.notNull(operation.getAccountId());

    OperationThrift operationThrift = new OperationThrift();
    operationThrift.setOperationId(operation.getOperationId());
    if (operation.getTargetId() != null) {
      operationThrift.setTargetId(operation.getTargetId());
    }
    operationThrift.setOperationTarget(operation.getTargetType().name());
    operationThrift.setStartTime(operation.getStartTime());
    operationThrift.setEndTime(operation.getEndTime());
    operationThrift.setDescription(operation.getDescription());
    operationThrift.setStatus(operation.getStatus().name());
    Long progress = operation.getProgress();
    if (progress != null) {
      operationThrift.setProgress(progress);
    } else {
      operationThrift.setProgress(0L);
    }
    operationThrift.setErrorMessage(operation.getErrorMessage());
    operationThrift.setAccountId(operation.getAccountId());
    operationThrift.setAccountName(operation.getAccountName());
    operationThrift.setOperationType(operation.getOperationType().name());
    operationThrift.setOperationObject(operation.getOperationObject());
    operationThrift.setTargetName(operation.getTargetName());
    Long targetSourceSize = operation.getTargetSourceSize();
    if (targetSourceSize != null) {
      operationThrift.setTargetSourceSize(targetSourceSize);
    } else {
      operationThrift.setTargetSourceSize(0L);
    }
    if (operation.getEndPointList() == null || operation.getEndPointList().isEmpty()) {
      operationThrift.setEndPointListString(null);
    } else {
      List<String> endPointListString = new ArrayList<>();
      for (EndPoint endPoint : operation.getEndPointList()) {
        if (endPoint == null) {
          continue;
        }
        endPointListString.add(endPoint.toString());
      }
      operationThrift.setEndPointListString(endPointListString);
    }
    Integer snapshotId = operation.getSnapshotId();
    if (snapshotId != null) {
      operationThrift.setSnapshotId(snapshotId);
    } else {
      operationThrift.setSnapshotId(0);
    }

    return operationThrift;
  }

  public static DatanodeTypeThrift convertDatanodeType2DatanodeTypeThrift(
      InstanceMetadata.DatanodeType datanodeType)
      throws DatanodeTypeNotSetExceptionThrift {
    if (datanodeType == InstanceMetadata.DatanodeType.NORMAL) {
      return DatanodeTypeThrift.NORMAL;

    } else if (datanodeType == InstanceMetadata.DatanodeType.SIMPLE) {
      return DatanodeTypeThrift.SIMPLE;
    } else {
      logger.error("datanode type not set");
      throw new DatanodeTypeNotSetExceptionThrift().setDetail("datanode type not set");
    }
  }

  public static InstanceMetadata.DatanodeType convertDatanodeTypeThrift2DatanodeType(
      DatanodeTypeThrift datanodeTypeThrift) throws DatanodeTypeNotSetExceptionThrift {
    if (datanodeTypeThrift == DatanodeTypeThrift.NORMAL) {
      return InstanceMetadata.DatanodeType.NORMAL;
    } else if (datanodeTypeThrift == DatanodeTypeThrift.SIMPLE) {
      return InstanceMetadata.DatanodeType.SIMPLE;
    } else {
      logger.error("datanode type not set");
      throw new DatanodeTypeNotSetExceptionThrift().setDetail("datanode type not set");
    }
  }

  public static EventLogInfoThrift buildEventLogInfoThriftFrom(EventLogInfo eventLogInfo) {
    EventLogInfoThrift eventLogInfoThrift = new EventLogInfoThrift();
    eventLogInfoThrift.setId(eventLogInfo.getId());
    eventLogInfoThrift.setStartTime(eventLogInfo.getStartTime().getTime());
    eventLogInfoThrift.setEventLog(eventLogInfo.getEventLog());

    return eventLogInfoThrift;
  }

  public static EventLogInfo buildEventLogInfoFrom(EventLogInfoThrift eventLogInfoThrift) {
    EventLogInfo eventLogInfo = new EventLogInfo();
    eventLogInfo.setId(eventLogInfoThrift.getId());
    eventLogInfo.setStartTime(new Date(eventLogInfoThrift.getStartTime()));
    eventLogInfo.setEventLog(eventLogInfoThrift.getEventLog());

    return eventLogInfo;
  }

  private static AbsoluteTimethrift convertRebalanceAbsoluteTime2AbsoluteTimeThrift(
      RebalanceAbsoluteTime absoluteTime) {
    if (absoluteTime == null) {
      return null;
    }
    AbsoluteTimethrift absoluteTimeThrift = new AbsoluteTimethrift();
    absoluteTimeThrift.setId(absoluteTime.getId());
    absoluteTimeThrift.setBeginTime(absoluteTime.getBeginTime());
    absoluteTimeThrift.setEndTime(absoluteTime.getEndTime());

    Set<WeekDaythrift> weekDayThriftSet = null;
    if (absoluteTime.getWeekDaySet() != null) {
      weekDayThriftSet = new HashSet<>();
      for (RebalanceAbsoluteTime.WeekDay weekDay : absoluteTime.getWeekDaySet()) {
        WeekDaythrift weekDayThrift;
        switch (weekDay.getValue()) {
          case 0:
            weekDayThrift = WeekDaythrift.SUN;
            break;
          case 1:
            weekDayThrift = WeekDaythrift.MON;
            break;
          case 2:
            weekDayThrift = WeekDaythrift.TUE;
            break;
          case 3:
            weekDayThrift = WeekDaythrift.WED;
            break;
          case 4:
            weekDayThrift = WeekDaythrift.THU;
            break;
          case 5:
            weekDayThrift = WeekDaythrift.FRI;
            break;
          case 6:
            weekDayThrift = WeekDaythrift.SAT;
            break;
          default:
            weekDayThrift = null;
            break;
        }
        if (weekDayThrift != null) {
          weekDayThriftSet.add(weekDayThrift);
        }
      }
    }
    absoluteTimeThrift.setWeekDay(weekDayThriftSet);

    return absoluteTimeThrift;
  }

  private static RebalanceAbsoluteTime convertRebalanceAbsoluteTimeThrift2AbsoluteTime(
      AbsoluteTimethrift absoluteTimeThrift) {
    if (absoluteTimeThrift == null) {
      return null;
    }

    RebalanceAbsoluteTime absoluteTime = new RebalanceAbsoluteTime();
    absoluteTime.setId(absoluteTimeThrift.getId());
    absoluteTime.setBeginTime(absoluteTimeThrift.getBeginTime());
    absoluteTime.setEndTime(absoluteTimeThrift.getEndTime());

    Set<RebalanceAbsoluteTime.WeekDay> weekDaySet = null;
    if (absoluteTimeThrift.getWeekDay() != null) {
      weekDaySet = new HashSet<>();
      for (WeekDaythrift weekDayThrift : absoluteTimeThrift.getWeekDay()) {
        RebalanceAbsoluteTime.WeekDay weekDay;
        switch (weekDayThrift.getValue()) {
          case 0:
            weekDay = RebalanceAbsoluteTime.WeekDay.SUN;
            break;
          case 1:
            weekDay = RebalanceAbsoluteTime.WeekDay.MON;
            break;
          case 2:
            weekDay = RebalanceAbsoluteTime.WeekDay.TUE;
            break;
          case 3:
            weekDay = RebalanceAbsoluteTime.WeekDay.WED;
            break;
          case 4:
            weekDay = RebalanceAbsoluteTime.WeekDay.THU;
            break;
          case 5:
            weekDay = RebalanceAbsoluteTime.WeekDay.FRI;
            break;
          case 6:
            weekDay = RebalanceAbsoluteTime.WeekDay.SAT;
            break;
          default:
            weekDay = null;
            break;
        }
        if (weekDay != null) {
          weekDaySet.add(weekDay);
        }
      }
    }
    absoluteTime.setWeekDaySet(weekDaySet);

    return absoluteTime;
  }

  public static RebalanceRule convertRebalanceRuleThrift2RebalanceRule(
      RebalanceRulethrift rebalanceRuleThrift) {
    if (rebalanceRuleThrift == null) {
      return null;
    }

    RebalanceRule rebalanceRule = new RebalanceRule();
    rebalanceRule.setRuleId(rebalanceRuleThrift.getRuleId());
    rebalanceRule.setRuleName(rebalanceRuleThrift.getRuleName());

    //relative time
    RebalanceRelativeTime relativeTime = new RebalanceRelativeTime();
    long waitTime = RebalanceRule.MIN_WAIT_TIME;
    if (rebalanceRuleThrift.getRelativeTime() != null) {
      waitTime = rebalanceRuleThrift.getRelativeTime().getWaitTime();
    }
    if (waitTime < RebalanceRule.MIN_WAIT_TIME) {
      waitTime = RebalanceRule.MIN_WAIT_TIME;
    }
    relativeTime.setWaitTime(waitTime);
    rebalanceRule.setRelativeTime(relativeTime);

    //absolute time
    List<RebalanceAbsoluteTime> absoluteTimeList = new ArrayList<>();
    List<AbsoluteTimethrift> absoluteTimeThriftList = rebalanceRuleThrift.getAbsoluteTimeList();
    if (absoluteTimeThriftList != null) {
      for (AbsoluteTimethrift absoluteTimeThrift : absoluteTimeThriftList) {
        RebalanceAbsoluteTime rebalanceAbsoluteTime =
            convertRebalanceAbsoluteTimeThrift2AbsoluteTime(
                absoluteTimeThrift);
        if (rebalanceAbsoluteTime == null) {
          continue;
        }
        absoluteTimeList.add(rebalanceAbsoluteTime);
      }
    }
    rebalanceRule.setAbsoluteTimeList(absoluteTimeList);

    return rebalanceRule;
  }

  public static RebalanceRulethrift convertRebalanceRule2RebalanceRuleThrift(
      RebalanceRule rebalanceRule) {
    if (rebalanceRule == null) {
      return null;
    }

    RebalanceRulethrift rebalanceRuleThrift = new RebalanceRulethrift();
    rebalanceRuleThrift.setRuleId(rebalanceRule.getRuleId());
    rebalanceRuleThrift.setRuleName(rebalanceRule.getRuleName());

    //relative time
    RelativeTimethrift relativeTimeThrift = null;
    if (rebalanceRule.getRelativeTime() != null) {
      relativeTimeThrift = new RelativeTimethrift();
      long waitTime = rebalanceRule.getRelativeTime().getWaitTime();
      if (waitTime < RebalanceRule.MIN_WAIT_TIME) {
        waitTime = RebalanceRule.MIN_WAIT_TIME;
      }
      relativeTimeThrift.setWaitTime(waitTime);
    }
    rebalanceRuleThrift.setRelativeTime(relativeTimeThrift);

    //absolute time
    List<AbsoluteTimethrift> absoluteTimeThriftList = new ArrayList<>();
    List<RebalanceAbsoluteTime> absoluteTimeList = rebalanceRule.getAbsoluteTimeList();
    for (RebalanceAbsoluteTime absoluteTime : absoluteTimeList) {
      AbsoluteTimethrift absoluteTimeThrift = convertRebalanceAbsoluteTime2AbsoluteTimeThrift(
          absoluteTime);
      if (absoluteTimeThrift == null) {
        continue;
      }
      absoluteTimeThriftList.add(absoluteTimeThrift);
    }
    rebalanceRuleThrift.setAbsoluteTimeList(absoluteTimeThriftList);

    return rebalanceRuleThrift;
  }

  /**
   * convert {@link IscsiAccessRuleThrift} to {@link py.driver.IscsiAccessRule}.
   */
  public static py.driver.IscsiAccessRule convertIscsiAccessRuleThrift2IscsiAccessRule(
      IscsiAccessRuleThrift accessRule) {
    if (accessRule == null) {
      return null;
    }
    py.driver.IscsiAccessRule iscsiAccessRule = new py.driver.IscsiAccessRule(
        accessRule.getInitiatorName(),
        accessRule.getUser(), accessRule.getPassed(), accessRule.getOutUser(),
        accessRule.getOutPassed());
    return iscsiAccessRule;
  }
}
