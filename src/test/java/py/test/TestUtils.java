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

package py.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static py.volume.VolumeInAction.NULL;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.apache.commons.lang.math.RandomUtils;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.RequestResponseHelper;
import py.archive.ArchiveMetadata;
import py.archive.ArchiveOptions;
import py.archive.ArchiveType;
import py.archive.RawArchiveMetadata;
import py.archive.StorageType;
import py.archive.brick.BrickMetadata;
import py.archive.brick.BrickStatus;
import py.archive.segment.SegId;
import py.archive.segment.SegmentMetadata;
import py.archive.segment.SegmentUnitMetadata;
import py.archive.segment.SegmentUnitStatus;
import py.archive.segment.SegmentUnitType;
import py.archive.segment.SegmentVersion;
import py.common.Constants;
import py.common.RequestIdBuilder;
import py.common.Utils;
import py.common.VolumeMetadataJsonParser;
import py.common.struct.EndPoint;
import py.common.struct.Pair;
import py.driver.DriverType;
import py.icshare.AccessRuleStatusBindingVolume;
import py.icshare.AccountMetadata;
import py.icshare.CapacityRecord;
import py.icshare.Domain;
import py.icshare.TotalAndUsedCapacity;
import py.icshare.Volume2AccessRuleRelationship;
import py.icshare.VolumeAccessRule;
import py.icshare.VolumeRecycleInformation;
import py.icshare.authorization.ApiToAuthorize;
import py.icshare.authorization.ApiToAuthorize.ApiCategory;
import py.icshare.authorization.PyResource;
import py.icshare.authorization.Role;
import py.icshare.iscsiaccessrule.Iscsi2AccessRuleRelationship;
import py.icshare.iscsiaccessrule.IscsiAccessRule;
import py.informationcenter.AccessPermissionType;
import py.informationcenter.AccessRuleStatus;
import py.informationcenter.StoragePool;
import py.informationcenter.StoragePoolStrategy;
import py.instance.InstanceId;
import py.membership.SegmentMembership;
import py.storage.Storage;
import py.storage.impl.DummyStorage;
import py.thrift.share.AccountMetadataBackupThrift;
import py.thrift.share.ApiToAuthorizeThrift;
import py.thrift.share.CapacityRecordThrift;
import py.thrift.share.DomainThrift;
import py.thrift.share.GroupThrift;
import py.thrift.share.IscsiAccessRuleThrift;
import py.thrift.share.IscsiRuleRelationshipThrift;
import py.thrift.share.ReportDbRequestThrift;
import py.thrift.share.ReportDbResponseThrift;
import py.thrift.share.ResourceThrift;
import py.thrift.share.RoleThrift;
import py.thrift.share.StoragePoolThrift;
import py.thrift.share.VolumeAccessRuleThrift;
import py.thrift.share.VolumeRecycleInformationThrift;
import py.thrift.share.VolumeRuleRelationshipThrift;
import py.volume.CacheType;
import py.volume.VolumeMetadata;
import py.volume.VolumeStatus;
import py.volume.VolumeType;

public class TestUtils {
  public static Logger logger = LoggerFactory.getLogger(TestUtils.class);

  public static RawArchiveMetadata generateArchiveMetadata(int pageSize, long segUnitSize) {
    ArchiveMetadata archiveMetadataBase = new ArchiveMetadata();
    archiveMetadataBase.setCreatedTime(System.currentTimeMillis());
    String user = System.getProperty("user.name");
    archiveMetadataBase.setUpdatedBy(user != null ? user : "unknown");
    archiveMetadataBase.setPageSize(pageSize);
    archiveMetadataBase.setSerialNumber(String.valueOf(System.currentTimeMillis()));
    archiveMetadataBase.setArchiveType(ArchiveType.RAW_DISK);
    archiveMetadataBase.setStorageType(StorageType.PCIE);
    RawArchiveMetadata archiveMetadata = new RawArchiveMetadata(archiveMetadataBase);
    archiveMetadata.setSegmentUnitSize(segUnitSize);
    archiveMetadata.setPageIndexersRegionSize(1024L * 1024);
    return archiveMetadata;
  }

  public static SegmentMembership generateMembership(SegmentVersion version) {
    return generateMembership(version, 1L, 2L, 3L);
  }

  public static SegmentMembership generateMembership() {
    return generateMembership(new SegmentVersion(1, 0), 1L, 2L, 3L);
  }

  public static SegmentMembership generateMembership(VolumeType volumeType) {
    List<InstanceId> secondaries = new ArrayList<InstanceId>(volumeType.getNumSecondaries());
    List<InstanceId> arbiters = new ArrayList<InstanceId>(volumeType.getNumArbiters());

    long primaryId = 1L;
    long secondaryId = primaryId;
    for (int i = 0; i < volumeType.getNumSecondaries(); i++) {
      secondaryId++;
      secondaries.add(new InstanceId(secondaryId));
    }

    for (int i = 0; i < volumeType.getNumArbiters(); i++) {
      secondaryId++;
      arbiters.add(new InstanceId(secondaryId));
    }

    return new SegmentMembership(new SegmentVersion(1, 1), new InstanceId(primaryId),
        new InstanceId(primaryId),
        secondaries, arbiters, null, null, null, null);
  }

  public static SegmentMembership generateMembership(SegmentVersion version, Long primary,
      Long... secondaryIds) {
    // the first one is the primary, and rest of them are secondaries
    Validate.isTrue(secondaryIds.length >= 2);
    List<InstanceId> secondaries = new ArrayList<InstanceId>();
    for (int i = 0; i < secondaryIds.length; i++) {
      if (secondaryIds[i] != primary) {
        secondaries.add(new InstanceId(secondaryIds[i]));
      }
    }
    return new SegmentMembership(version, new InstanceId(primary), secondaries);
  }

  public static SegmentUnitMetadata generateSegmentUnitMetadata(SegId segId,
      SegmentUnitStatus status) {
    return generateSegmentUnitMetadata(segId, status, 0, VolumeType.REGULAR,
        SegmentUnitType.Normal);
  }

  public static SegmentUnitMetadata generateSegmentUnitMetadata(SegId segId,
      SegmentUnitStatus status,
      VolumeType volumeType) {
    return generateSegmentUnitMetadata(segId, status, 0, volumeType, SegmentUnitType.Normal);
  }

  public static SegmentUnitMetadata generateSegmentUnitMetadata(SegId segId,
      SegmentUnitStatus status,
      VolumeType volumeType, SegmentUnitType unitType) {
    return generateSegmentUnitMetadata(segId, status, 0, volumeType, unitType);
  }

  public static SegmentUnitMetadata generateSegmentUnitMetadata(SegId segId,
      SegmentUnitStatus status,
      long startOffsetInArchive, VolumeType volumeType, SegmentUnitType unitType) {
    SegmentUnitMetadata newMetadata = new SegmentUnitMetadata(segId, startOffsetInArchive,
        generateMembership(volumeType), status, volumeType, unitType);
    Storage storage = new DummyStorage("dummpy Storage", ArchiveOptions.SEGMENTUNIT_METADATA_LENGTH
        + ArchiveOptions.SEGMENTUNIT_BITMAP_LENGTH + 4096);
    newMetadata.setStorage(storage);
    newMetadata.setMetadataOffsetInArchive(0);
    newMetadata.setStatus(status);

    if (unitType.equals(SegmentUnitType.Normal)) {
      int metadataOffset = ArchiveOptions.SEGMENTUNIT_METADATA_LENGTH * 100;
      int offset = ArchiveOptions.BRICK_METADATA_LENGTH * 100 + metadataOffset;
      BrickMetadata brickMetadata = new BrickMetadata(segId, offset, metadataOffset,
          BrickStatus.allocated, storage, 0, 0);
      newMetadata.setBrickMetadata(brickMetadata);
    }
    //newMetadata.updateSnapshotManager(new GenericVolumeSnapshotManager(0L), false);
    return newMetadata;
  }

  public static SegmentMetadata generateSegmentMetadata(SegId segId) {
    return generateSegmentMetadata(segId, VolumeType.REGULAR);
  }

  public static SegmentMetadata generateSegmentMetadata(SegId segId, VolumeType volumeType) {
    SegmentMetadata segmentMetadata = new SegmentMetadata(segId, segId.getIndex());
    SegmentUnitMetadata segmentUnitMetadataPrimary = TestUtils
        .generateSegmentUnitMetadata(segId, SegmentUnitStatus.Primary, volumeType);
    segmentUnitMetadataPrimary.setLastReported(System.currentTimeMillis());
    // get the primary id from the membership
    SegmentMembership currentMembership = segmentUnitMetadataPrimary.getMembership();
    InstanceId primaryId = currentMembership.getPrimary();
    segmentUnitMetadataPrimary.setInstanceId(primaryId);
    segmentMetadata.putSegmentUnitMetadata(primaryId, segmentUnitMetadataPrimary);

    ArrayList<InstanceId> secondariesId = new ArrayList<>(currentMembership.getSecondaries());
    for (int i = 0; i < volumeType.getNumSecondaries(); i++) {
      SegmentUnitMetadata segmentUnitMetadataSecondary = TestUtils
          .generateSegmentUnitMetadata(segId, SegmentUnitStatus.Secondary, volumeType,
              SegmentUnitType.Normal);
      segmentUnitMetadataSecondary.setLastReported(System.currentTimeMillis());
      segmentUnitMetadataSecondary.setInstanceId(new InstanceId(secondariesId.get(i)));
      segmentMetadata.putSegmentUnitMetadata(new InstanceId(secondariesId.get(i)),
          segmentUnitMetadataSecondary);
    }

    ArrayList<InstanceId> arbitersId = new ArrayList<>(currentMembership.getArbiters());
    for (int i = 0; i < volumeType.getNumArbiters(); i++) {
      SegmentUnitMetadata segmentUnitMetadataSecondary = TestUtils
          .generateSegmentUnitMetadata(segId, SegmentUnitStatus.Arbiter, volumeType,
              SegmentUnitType.Arbiter);
      segmentUnitMetadataSecondary.setLastReported(System.currentTimeMillis());
      segmentUnitMetadataSecondary.setInstanceId(new InstanceId(arbitersId.get(i)));
      segmentMetadata
          .putSegmentUnitMetadata(new InstanceId(arbitersId.get(i)), segmentUnitMetadataSecondary);
    }

    return segmentMetadata;
  }

  public static VolumeMetadata generateVolumeMetadata() {
    return generateVolumeMetadata(VolumeType.REGULAR);
  }

  public static VolumeMetadata generateVolumeMetadata(VolumeType volumeType) {
    VolumeMetadata volumeMetadata = new VolumeMetadata();
    volumeMetadata.setAccountId(buildId());
    volumeMetadata.setVolumeId(buildId());
    volumeMetadata.setRootVolumeId(volumeMetadata.getVolumeId());
    volumeMetadata.setName("volume_test");
    volumeMetadata.setVolumeSize(0);
    volumeMetadata.setVolumeType(volumeType);
    volumeMetadata.setVolumeStatus(VolumeStatus.Available);
    volumeMetadata.setVersion(1);
    SegmentMetadata segmentMetadata1 = generateSegmentMetadata(
        new SegId(volumeMetadata.getVolumeId(), 0),
        volumeType);
    SegmentMetadata segmentMetadata2 = generateSegmentMetadata(
        new SegId(volumeMetadata.getVolumeId(), 1),
        volumeType);
    volumeMetadata.addSegmentMetadata(segmentMetadata1, segmentMetadata1.getLatestMembership());
    volumeMetadata.addSegmentMetadata(segmentMetadata2, segmentMetadata2.getLatestMembership());

    return volumeMetadata;
  }

  public static VolumeMetadata generateVolumeMetadataWithSingleSegment(VolumeType volumeType) {
    VolumeMetadata volumeMetadata = new VolumeMetadata();
    volumeMetadata.setAccountId(buildId());
    volumeMetadata.setVolumeId(buildId());
    volumeMetadata.setRootVolumeId(volumeMetadata.getVolumeId());
    volumeMetadata.setName("volume_test");
    volumeMetadata.setVolumeSize(0);
    volumeMetadata.setVolumeType(volumeType);
    volumeMetadata.setVolumeStatus(VolumeStatus.Available);
    volumeMetadata.setInAction(NULL);
    volumeMetadata.setVersion(1);
    volumeMetadata.setDomainId(1L);
    volumeMetadata.setVolumeSource(VolumeMetadata.VolumeSourceType.CREATE_VOLUME);
    SegmentMetadata segmentMetadata = generateSegmentMetadata(
        new SegId(volumeMetadata.getVolumeId(), 0),
        volumeType);
    volumeMetadata.addSegmentMetadata(segmentMetadata, segmentMetadata.getLatestMembership());

    return volumeMetadata;
  }

  public static VolumeMetadataJsonParser generateVolumeMetadataJsonParser(int version)
      throws JsonProcessingException {
    return generateVolumeMetadataJsonParser(version, null);
  }

  public static VolumeMetadataJsonParser generateVolumeMetadataJsonParser(int version,
      VolumeMetadata volumeMetadata)
      throws JsonProcessingException {
    if (version == -1) {
      return new VolumeMetadataJsonParser(null);
    } else {
      if (volumeMetadata == null) {
        volumeMetadata = generateVolumeMetadata();
      }
      volumeMetadata.setVersion(version);

      return new VolumeMetadataJsonParser(version, volumeMetadata.toJsonString());
    }
  }

  public static VolumeMetadataJsonParser generateVolumeMetadataJsonParser(
      VolumeMetadata volumeMetadata)
      throws JsonProcessingException {
    if (volumeMetadata == null) {
      volumeMetadata = generateVolumeMetadata();
    }

    return new VolumeMetadataJsonParser(volumeMetadata.getVersion(), volumeMetadata.toJsonString());
  }

  public static long buildId() {
    return RequestIdBuilder.get();
  }

  public static byte[] generateDataAssociatedWithAddress(long offset, int length) {
    if (length % 8 != 0) {
      fail("the length has to be multiple times of 8" + "offset:" + offset + "length:" + length);
    }

    byte[] src = new byte[length];
    ByteBuffer outputBuf = ByteBuffer.wrap(src);
    int times = length / 8;
    for (int i = 0; i < times; i++) {
      outputBuf.putLong(offset);
      offset += 8;
    }

    return src;
  }

  public static void checkDataAssociatedWithAddress(long offset, byte[] src, int length) {
    logger.debug("verifying offset:{} length:{}", offset, length);
    long origOffset = offset;
    if (length % 8 != 0) {
      fail("the length has to be multiple times of 8" + " offset:" + offset + "length:" + length);
    }
    int times = length / 8;
    byte[] temp = new byte[8];
    for (int i = 0; i < times; i++) {
      for (int j = 0; j < 8; j++) {
        temp[j] = src[i * 8 + j];
      }

      long returnedValue = ByteUtils.bytesToLong(temp);
      if (returnedValue != offset) {
        logger.warn(
            "Failed to verify offset:{} length:{} the error occur at the offset:{} returned value "
                + "is {}",
            origOffset, length, offset, returnedValue);
        StringBuilder builder = new StringBuilder();
        int offsetInArrayToBuildString = i * 8 - 16;
        if (offsetInArrayToBuildString < 0) {
          offsetInArrayToBuildString = 0;
        }
        Utils.toString(ByteBuffer.wrap(src, offsetInArrayToBuildString, 1024), builder);
        logger.error("something wrong {}", builder.toString());
        fail();
      }
      offset += 8;
    }
  }

  public static byte[] generateRandomData(int length) {
    byte[] src = new byte[length];

    Random random = new Random(System.currentTimeMillis());
    random.nextBytes(src);

    return src;
  }

  @SuppressWarnings("resource")
  public static void truncateFile(File f, long newSize) throws IOException {
    FileChannel outChan = new FileOutputStream(f, true).getChannel();
    outChan.truncate(newSize);
    outChan.close();
  }

  public static Domain buildDomain() {
    Domain domain = new Domain();
    domain.setDomainId(RequestIdBuilder.get());
    domain.setDomainDescription(null);
    domain.setDomainName(TestBase.getRandomString(10));
    domain.setDataNodes(buildIdSet(3));
    domain.setStoragePools(buildIdSet(5));
    return domain;
  }

  public static ApiToAuthorize buildApi() {
    return new ApiToAuthorize("apiName", ApiCategory.Volume.name());
  }

  public static PyResource buildResource() {
    return new PyResource(1L, "resourceName", PyResource.ResourceType.Volume.name());
  }

  public static Role buildRole() {
    Role role = new Role(1L, "testForBackup");
    role.addPermission(buildApi());
    return role;
  }

  public static AccountMetadata buildAccountMetadata() {
    AccountMetadata account = new AccountMetadata("testForBackup", "312",
        AccountMetadata.AccountType.SuperAdmin.name(), 1L);
    Set<Role> roles = new HashSet<>();
    roles.add(buildRole());
    account.setRoles(roles);
    Set<PyResource> resources = new HashSet<>();
    resources.add(buildResource());
    account.setResources(resources);
    return account;
  }

  public static StoragePool buildStoragePool() {
    StoragePool storagePool = new StoragePool();
    storagePool.setDomainId(RequestIdBuilder.get());
    storagePool.setPoolId(RequestIdBuilder.get());
    storagePool.setName(TestBase.getRandomString(6));
    storagePool.setDescription(TestBase.getRandomString(15));
    storagePool.setStrategy(StoragePoolStrategy.Capacity);
    storagePool.setArchivesInDataNode(buildMultiMap(4));
    storagePool.setVolumeIds(buildIdSet(3));
    storagePool.setStoragePoolLevel("HIGH");
    storagePool.setDomainName("domain");
    storagePool.setLastUpdateTime(0L);
    return storagePool;
  }

  public static CapacityRecord buildCapacityRecord() {
    CapacityRecord recordInfo = new CapacityRecord();
    int recordDaysCount = 30;
    for (int i = 0; i <= recordDaysCount; i++) {
      TotalAndUsedCapacity capacityInfo = new TotalAndUsedCapacity(RequestIdBuilder.get(),
          RequestIdBuilder.get());
      Date nowDate = new Date();
      SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
      String dateString = dateFormat.format(nowDate);
      recordInfo.getRecordMap().put(dateString, capacityInfo);
    }
    return recordInfo;
  }

  public static Iscsi2AccessRuleRelationship buildIscsi2AccessRuleRelationship() {
    Iscsi2AccessRuleRelationship iscsi2Rule = new Iscsi2AccessRuleRelationship();
    iscsi2Rule.setRuleId(RequestIdBuilder.get());
    iscsi2Rule.setRelationshipId(RequestIdBuilder.get());
    iscsi2Rule.setDriverContainerId(RequestIdBuilder.get());
    iscsi2Rule.setVolumeId(RequestIdBuilder.get());
    iscsi2Rule.setSnapshotId(0);
    iscsi2Rule.setDriverType(DriverType.ISCSI.name());
    iscsi2Rule.setStatus(AccessRuleStatusBindingVolume.APPLIED);
    return iscsi2Rule;
  }

  public static IscsiAccessRule buildIscsiAccessRule() {
    IscsiAccessRule accessRule = new IscsiAccessRule();
    accessRule.setRuleId(RequestIdBuilder.get());
    accessRule.setInitiatorName(TestBase.getRandomString(10));
    accessRule.setUser(TestBase.getRandomString(10));
    accessRule.setPassed(TestBase.getRandomString(10));
    accessRule.setPermission(AccessPermissionType.READWRITE);
    accessRule.setStatus(AccessRuleStatus.AVAILABLE);
    return accessRule;
  }

  public static Volume2AccessRuleRelationship buildVolume2AccessRuleRelationship() {
    Volume2AccessRuleRelationship volume2Rule = new Volume2AccessRuleRelationship();
    volume2Rule.setRuleId(RequestIdBuilder.get());
    volume2Rule.setRelationshipId(RequestIdBuilder.get());
    volume2Rule.setVolumeId(RequestIdBuilder.get());
    volume2Rule.setStatus(AccessRuleStatusBindingVolume.APPLIED);
    return volume2Rule;
  }

  public static VolumeAccessRule buildVolumeAccessRule() {
    VolumeAccessRule accessRule = new VolumeAccessRule();
    accessRule.setRuleId(RequestIdBuilder.get());
    accessRule.setIncommingHostName(TestBase.getRandomString(10));
    accessRule.setPermission(AccessPermissionType.READWRITE);
    accessRule.setStatus(AccessRuleStatus.AVAILABLE);
    return accessRule;
  }

  public static VolumeRecycleInformation buildVolumeRecycleInformation() {
    long volumeId = 11111L;
    VolumeRecycleInformation volumeRecycleInformation = new VolumeRecycleInformation();
    volumeRecycleInformation.setVolumeId(volumeId);
    volumeRecycleInformation.setTimeInRecycle(123456);
    return volumeRecycleInformation;
  }

  public static ReportDbResponseThrift buildReportDbResponse(Long sequenceId, int domainCount,
      int storagePoolCount,
      int volume2RuleCount, int accessRuleCount, int capacityRecordCount,
      int roleCount, int accountCount, int apiCount, int resourceCount) {
    ReportDbResponseThrift response = new ReportDbResponseThrift();

    // set sequenceId
    response.setSequenceId(sequenceId);

    // domain
    if (domainCount > 0) {
      List<DomainThrift> domainThriftList = new ArrayList<>();
      for (int i = 0; i < domainCount; i++) {
        DomainThrift domainThrift = RequestResponseHelper.buildDomainThriftFrom(buildDomain());
        domainThriftList.add(domainThrift);
      }
      response.setDomainThriftList(domainThriftList);
    }

    // storagePool
    if (storagePoolCount > 0) {
      List<StoragePoolThrift> storagePoolThriftList = new ArrayList<>();
      for (int i = 0; i < storagePoolCount; i++) {
        StoragePoolThrift storagePoolThrift = RequestResponseHelper
            .buildThriftStoragePoolFrom(buildStoragePool(), null);
        storagePoolThriftList.add(storagePoolThrift);
      }
      response.setStoragePoolThriftList(storagePoolThriftList);
    }
    // volume2Rule
    if (volume2RuleCount > 0) {
      List<VolumeRuleRelationshipThrift> volume2RuleThriftList = new ArrayList<>();
      for (int i = 0; i < volume2RuleCount; i++) {
        VolumeRuleRelationshipThrift volume2Rule = RequestResponseHelper
            .buildThriftVolumeRuleRelationship(buildVolume2AccessRuleRelationship());
        volume2RuleThriftList.add(volume2Rule);
      }
      response.setVolume2RuleThriftList(volume2RuleThriftList);
    }
    // accessRule
    if (accessRuleCount > 0) {
      List<VolumeAccessRuleThrift> accessRuleThriftList = new ArrayList<>();
      for (int i = 0; i < accessRuleCount; i++) {
        VolumeAccessRuleThrift accessRuleThrfit = RequestResponseHelper
            .buildVolumeAccessRuleThriftFrom(buildVolumeAccessRule());
        accessRuleThriftList.add(accessRuleThrfit);
      }
      response.setAccessRuleThriftList(accessRuleThriftList);
    }
    // capacityRecord
    if (capacityRecordCount > 0) {
      List<CapacityRecordThrift> capacityRecordThriftList = new ArrayList<>();
      for (int i = 0; i < capacityRecordCount; i++) {
        CapacityRecordThrift capacityRecordThrift = RequestResponseHelper
            .buildThriftCapacityRecordFrom(buildCapacityRecord());
        capacityRecordThriftList.add(capacityRecordThrift);
      }
      response.setCapacityRecordThriftList(capacityRecordThriftList);
    }

    if (apiCount > 0) {
      List<ApiToAuthorizeThrift> apiThriftList = new ArrayList<>();
      for (int i = 0; i < apiCount; i++) {
        apiThriftList.add(RequestResponseHelper.buildApiToAuthorizeThrift(buildApi()));
      }
      response.setApiThriftList(apiThriftList);
    }

    if (resourceCount > 0) {
      List<ResourceThrift> resourceThriftList = new ArrayList<>();
      for (int i = 0; i < resourceCount; i++) {
        resourceThriftList.add(RequestResponseHelper.buildResourceThrift(buildResource()));
      }
      response.setResourceThriftList(resourceThriftList);
    }

    if (roleCount > 0) {
      List<RoleThrift> roleThriftList = new ArrayList<>();
      for (int i = 0; i < roleCount; i++) {
        roleThriftList.add(RequestResponseHelper.buildRoleThrift(buildRole()));
      }
      response.setRoleThriftList(roleThriftList);
    }

    if (accountCount > 0) {
      List<AccountMetadataBackupThrift> accountBackupThriftList = new ArrayList<>();
      for (int i = 0; i < accountCount; i++) {
        accountBackupThriftList.add(RequestResponseHelper.buildAccountMetadataBackupThrift(
            buildAccountMetadata()));
      }
      response.setAccountMetadataBackupThriftList(accountBackupThriftList);
    }
    return response;
  }

  public static ReportDbResponseThrift buildReportDbResponse(Long sequenceId, int domainCount,
      int storagePoolCount,
      int volume2RuleCount, int accessRuleCount, int capacityRecordCount,
      int roleCount, int accountCount, int apiCount, int resourceCount,
      int iscsi2RuleCount, int iscsiAccessRuleCount) {
    ReportDbResponseThrift response = new ReportDbResponseThrift();

    // set sequenceId
    response.setSequenceId(sequenceId);

    // domain
    if (domainCount > 0) {
      List<DomainThrift> domainThriftList = new ArrayList<>();
      for (int i = 0; i < domainCount; i++) {
        DomainThrift domainThrift = RequestResponseHelper.buildDomainThriftFrom(buildDomain());
        domainThriftList.add(domainThrift);
      }
      response.setDomainThriftList(domainThriftList);
    }

    // storagePool
    if (storagePoolCount > 0) {
      List<StoragePoolThrift> storagePoolThriftList = new ArrayList<>();
      for (int i = 0; i < storagePoolCount; i++) {
        StoragePoolThrift storagePoolThrift = RequestResponseHelper
            .buildThriftStoragePoolFrom(buildStoragePool(), null);
        storagePoolThriftList.add(storagePoolThrift);
      }
      response.setStoragePoolThriftList(storagePoolThriftList);
    }
    // volume2Rule
    if (volume2RuleCount > 0) {
      List<VolumeRuleRelationshipThrift> volume2RuleThriftList = new ArrayList<>();
      for (int i = 0; i < volume2RuleCount; i++) {
        VolumeRuleRelationshipThrift volume2Rule = RequestResponseHelper
            .buildThriftVolumeRuleRelationship(buildVolume2AccessRuleRelationship());
        volume2RuleThriftList.add(volume2Rule);
      }
      response.setVolume2RuleThriftList(volume2RuleThriftList);
    }
    // accessRule
    if (accessRuleCount > 0) {
      List<VolumeAccessRuleThrift> accessRuleThriftList = new ArrayList<>();
      for (int i = 0; i < accessRuleCount; i++) {
        VolumeAccessRuleThrift accessRuleThrfit = RequestResponseHelper
            .buildVolumeAccessRuleThriftFrom(buildVolumeAccessRule());
        accessRuleThriftList.add(accessRuleThrfit);
      }
      response.setAccessRuleThriftList(accessRuleThriftList);
    }

    // iscsi2Rule
    if (iscsi2RuleCount > 0) {
      List<IscsiRuleRelationshipThrift> iscsi2RuleThriftList = new ArrayList<>();
      for (int i = 0; i < volume2RuleCount; i++) {
        IscsiRuleRelationshipThrift iscsi2Rule = RequestResponseHelper
            .buildThriftIscsiRuleRelationship(buildIscsi2AccessRuleRelationship());
        iscsi2RuleThriftList.add(iscsi2Rule);
      }
      response.setIscsi2RuleThriftList(iscsi2RuleThriftList);
    }
    // iscsiAccessRule
    if (iscsiAccessRuleCount > 0) {
      List<IscsiAccessRuleThrift> iscsiAccessRuleThriftList = new ArrayList<>();
      for (int i = 0; i < iscsiAccessRuleCount; i++) {
        IscsiAccessRuleThrift iscsiAccessRuleThrfit = RequestResponseHelper
            .buildIscsiAccessRuleThriftFrom(buildIscsiAccessRule());
        iscsiAccessRuleThriftList.add(iscsiAccessRuleThrfit);
      }
      response.setIscsiAccessRuleThriftList(iscsiAccessRuleThriftList);
    }

    // capacityRecord
    if (capacityRecordCount > 0) {
      List<CapacityRecordThrift> capacityRecordThriftList = new ArrayList<>();
      for (int i = 0; i < capacityRecordCount; i++) {
        CapacityRecordThrift capacityRecordThrift = RequestResponseHelper
            .buildThriftCapacityRecordFrom(buildCapacityRecord());
        capacityRecordThriftList.add(capacityRecordThrift);
      }
      response.setCapacityRecordThriftList(capacityRecordThriftList);
    }

    if (apiCount > 0) {
      List<ApiToAuthorizeThrift> apiThriftList = new ArrayList<>();
      for (int i = 0; i < apiCount; i++) {
        apiThriftList.add(RequestResponseHelper.buildApiToAuthorizeThrift(buildApi()));
      }
      response.setApiThriftList(apiThriftList);
    }

    if (resourceCount > 0) {
      List<ResourceThrift> resourceThriftList = new ArrayList<>();
      for (int i = 0; i < resourceCount; i++) {
        resourceThriftList.add(RequestResponseHelper.buildResourceThrift(buildResource()));
      }
      response.setResourceThriftList(resourceThriftList);
    }

    if (roleCount > 0) {
      List<RoleThrift> roleThriftList = new ArrayList<>();
      for (int i = 0; i < roleCount; i++) {
        roleThriftList.add(RequestResponseHelper.buildRoleThrift(buildRole()));
      }
      response.setRoleThriftList(roleThriftList);
    }

    if (accountCount > 0) {
      List<AccountMetadataBackupThrift> accountBackupThriftList = new ArrayList<>();
      for (int i = 0; i < accountCount; i++) {
        accountBackupThriftList
            .add(RequestResponseHelper.buildAccountMetadataBackupThrift(buildAccountMetadata()));
      }
      response.setAccountMetadataBackupThriftList(accountBackupThriftList);
    }
    return response;
  }

  public static ReportDbRequestThrift buildReportDbRequest(int groupId, EndPoint endPoint,
      long sequenceId) {
    ReportDbRequestThrift reportDbRequestThrift = new ReportDbRequestThrift();
    reportDbRequestThrift.setSequenceId(sequenceId);
    GroupThrift groupThrift = new GroupThrift();
    groupThrift.setGroupId(groupId);
    reportDbRequestThrift.setGroup(groupThrift);
    reportDbRequestThrift.setEndpoint(endPoint.toString());
    // domain 3
    int domainCount = 3;
    List<DomainThrift> domainThriftList = new ArrayList<>();
    for (int i = 0; i < domainCount; i++) {
      DomainThrift domainThrift = RequestResponseHelper.buildDomainThriftFrom(buildDomain());
      domainThriftList.add(domainThrift);
    }
    reportDbRequestThrift.setDomainThriftList(domainThriftList);

    // storagePool 3
    int storagePoolCount = 3;
    List<StoragePoolThrift> storagePoolThriftList = new ArrayList<>();
    for (int i = 0; i < storagePoolCount; i++) {
      StoragePoolThrift storagePoolThrift = RequestResponseHelper
          .buildThriftStoragePoolFrom(buildStoragePool(), null);
      storagePoolThriftList.add(storagePoolThrift);
    }
    reportDbRequestThrift.setStoragePoolThriftList(storagePoolThriftList);

    // volume2Rule 3
    int volume2RuleCount = 3;
    List<VolumeRuleRelationshipThrift> volume2RuleThriftList = new ArrayList<>();
    for (int i = 0; i < volume2RuleCount; i++) {
      VolumeRuleRelationshipThrift volume2Rule = RequestResponseHelper
          .buildThriftVolumeRuleRelationship(buildVolume2AccessRuleRelationship());
      volume2RuleThriftList.add(volume2Rule);
    }
    reportDbRequestThrift.setVolume2RuleThriftList(volume2RuleThriftList);

    // accessRule 3
    int accessRuleCount = 3;
    List<VolumeAccessRuleThrift> accessRuleThriftList = new ArrayList<>();
    for (int i = 0; i < accessRuleCount; i++) {
      VolumeAccessRuleThrift accessRuleThrift = RequestResponseHelper
          .buildVolumeAccessRuleThriftFrom(buildVolumeAccessRule());
      accessRuleThriftList.add(accessRuleThrift);
    }
    reportDbRequestThrift.setAccessRuleThriftList(accessRuleThriftList);

    // iscsi2Rule 3
    int iscsi2RuleCount = 3;
    List<IscsiRuleRelationshipThrift> iscsi2RuleThriftList = new ArrayList<>();
    for (int i = 0; i < iscsi2RuleCount; i++) {
      IscsiRuleRelationshipThrift iscsi2Rule = RequestResponseHelper
          .buildThriftIscsiRuleRelationship(buildIscsi2AccessRuleRelationship());
      iscsi2RuleThriftList.add(iscsi2Rule);
    }
    reportDbRequestThrift.setIscsi2RuleThriftList(iscsi2RuleThriftList);

    // iscsiAccessRule 3
    int iscsiAccessRuleCount = 3;
    List<IscsiAccessRuleThrift> iscsiAccessRuleThriftList = new ArrayList<>();
    for (int i = 0; i < iscsiAccessRuleCount; i++) {
      IscsiAccessRuleThrift iscsiAccessRuleThrift = RequestResponseHelper
          .buildIscsiAccessRuleThriftFrom(buildIscsiAccessRule());
      iscsiAccessRuleThriftList.add(iscsiAccessRuleThrift);
    }
    reportDbRequestThrift.setIscsiAccessRuleThriftList(iscsiAccessRuleThriftList);

    // capacityRecord 1
    int capacityRecordCount = 1;
    List<CapacityRecordThrift> capacityRecordThriftList = new ArrayList<>();
    for (int i = 0; i < capacityRecordCount; i++) {
      CapacityRecordThrift capacityRecordThrift = RequestResponseHelper
          .buildThriftCapacityRecordFrom(buildCapacityRecord());
      capacityRecordThriftList.add(capacityRecordThrift);
    }
    reportDbRequestThrift.setCapacityRecordThriftList(capacityRecordThriftList);

    // api 1
    List<ApiToAuthorizeThrift> apiThriftList = new ArrayList<>();
    apiThriftList.add(RequestResponseHelper.buildApiToAuthorizeThrift(buildApi()));
    reportDbRequestThrift.setApiThriftList(apiThriftList);

    // resource 1
    List<ResourceThrift> resourceThriftList = new ArrayList<>();
    resourceThriftList.add(RequestResponseHelper.buildResourceThrift(buildResource()));
    reportDbRequestThrift.setResourceThriftList(resourceThriftList);

    // role 1
    List<RoleThrift> roleThriftList = new ArrayList<>();
    roleThriftList.add(RequestResponseHelper.buildRoleThrift(buildRole()));
    reportDbRequestThrift.setRoleThriftList(roleThriftList);

    // account 1
    List<AccountMetadataBackupThrift> accountBackupThriftList = new ArrayList<>();
    accountBackupThriftList
        .add(RequestResponseHelper.buildAccountMetadataBackupThrift(buildAccountMetadata()));
    reportDbRequestThrift.setAccountMetadataBackupThriftList(accountBackupThriftList);

    //volume_recycle
    List<VolumeRecycleInformationThrift> volumeRecycleInformationThrifts = new ArrayList<>();
    volumeRecycleInformationThrifts.add(RequestResponseHelper
        .buildThriftVolumeRecycleInformationFrom(buildVolumeRecycleInformation()));
    reportDbRequestThrift.setVolumeRecycleInformationThriftList(volumeRecycleInformationThrifts);

    return reportDbRequestThrift;
  }

  public static Set<Long> buildIdSet(int count) {
    Set<Long> idSet = new HashSet<>();
    for (int i = 0; i < count; i++) {
      idSet.add(RequestIdBuilder.get());
    }
    return idSet;
  }

  public static List<Long> buildIdList(int count) {
    List<Long> idList = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      idList.add(RequestIdBuilder.get());
    }
    return idList;
  }

  public static Multimap<Long, Long> buildMultiMap(int keyCount) {
    Multimap<Long, Long> buildMultiMap = Multimaps
        .synchronizedSetMultimap(HashMultimap.<Long, Long>create());
    for (int i = 0; i < keyCount; i++) {
      buildMultiMap.putAll(RequestIdBuilder.get(), buildIdSet(i));
    }
    return buildMultiMap;
  }

  public static Multimap<Long, Long> buildMultiMap(int datanodeCount, int archiveCount) {
    Multimap<Long, Long> buildMultiMap = Multimaps
        .synchronizedSetMultimap(HashMultimap.<Long, Long>create());
    for (int i = 0; i < datanodeCount; i++) {
      buildMultiMap.putAll(RequestIdBuilder.get(), buildIdSet(archiveCount));
    }
    return buildMultiMap;
  }

  public static Map<Long, Set<Long>> buildDatanodeToArchiveMap(int datanodeSize, int archiveSize) {
    Map<Long, Set<Long>> datanodeToArchiveMap = new HashMap<Long, Set<Long>>();
    for (int i = 0; i < datanodeSize; i++) {
      datanodeToArchiveMap.put(new Random().nextLong(), buildIdSet(archiveSize));
    }
    return datanodeToArchiveMap;
  }

  public static void compareReportRequestAndReportResponse(ReportDbRequestThrift reportRequest,
      ReportDbResponseThrift reportResponse) {
    // domain
    assertTrue(py.common.Utils
        .compareTwoList(reportRequest.getDomainThriftList(), reportResponse.getDomainThriftList()));
    // storagePool
    assertTrue(py.common.Utils
        .compareTwoList(reportRequest.getStoragePoolThriftList(),
            reportResponse.getStoragePoolThriftList()));
    // volume2Rule
    assertTrue(py.common.Utils
        .compareTwoList(reportRequest.getVolume2RuleThriftList(),
            reportResponse.getVolume2RuleThriftList()));
    // accessRule
    assertTrue(py.common.Utils.compareTwoList(reportRequest.getAccessRuleThriftList(),
        reportResponse.getAccessRuleThriftList()));
    // iscsi2Rule
    assertTrue(py.common.Utils.compareTwoList(reportRequest.getIscsi2RuleThriftList(),
        reportResponse.getIscsi2RuleThriftList()));
    // iscsiAccessRule
    assertTrue(py.common.Utils.compareTwoList(reportRequest.getIscsiAccessRuleThriftList(),
        reportResponse.getIscsiAccessRuleThriftList()));
    // capacityRecord
    assertTrue(py.common.Utils.compareTwoList(reportRequest.getCapacityRecordThriftList(),
        reportResponse.getCapacityRecordThriftList()));

    // api
    assertTrue(reportRequest.getApiThriftList().equals((reportResponse.getApiThriftList())));

    // resource
    assertTrue(
        reportRequest.getResourceThriftList().equals(reportResponse.getResourceThriftList()));

    // role
    assertEquals(reportRequest.getRoleThriftList(), reportResponse.getRoleThriftList());

    // account
    assertEquals(reportRequest.getAccountMetadataBackupThriftList(),
        reportResponse.getAccountMetadataBackupThriftList());
  }

  public static boolean compareTwoReportDbResponse(ReportDbResponseThrift reportResponse1,
      ReportDbResponseThrift reportResponse2) {
    // domain
    if (!py.common.Utils
        .compareTwoList(reportResponse1.getDomainThriftList(),
            reportResponse2.getDomainThriftList())) {
      return false;
    }
    // storagePool
    if (!py.common.Utils.compareTwoList(reportResponse1.getStoragePoolThriftList(),
        reportResponse2.getStoragePoolThriftList())) {
      return false;
    }
    // volume2Rule
    if (!py.common.Utils.compareTwoList(reportResponse1.getVolume2RuleThriftList(),
        reportResponse2.getVolume2RuleThriftList())) {
      return false;
    }
    // accessRule
    if (!py.common.Utils
        .compareTwoList(reportResponse1.getAccessRuleThriftList(),
            reportResponse2.getAccessRuleThriftList())) {
      return false;
    }
    // iscsi2Rule
    if (!py.common.Utils.compareTwoList(reportResponse1.getIscsi2RuleThriftList(),
        reportResponse2.getIscsi2RuleThriftList())) {
      return false;
    }
    // iscsiAccessRule
    if (!py.common.Utils.compareTwoList(reportResponse1.getIscsiAccessRuleThriftList(),
        reportResponse2.getIscsiAccessRuleThriftList())) {
      return false;
    }
    // capacityRecord
    if (!py.common.Utils.compareTwoList(reportResponse1.getCapacityRecordThriftList(),
        reportResponse2.getCapacityRecordThriftList())) {
      return false;
    }

    return true;
  }

  public static class ByteUtils {
    public static byte[] longToBytes(long x) {
      ByteBuffer buffer = ByteBuffer.allocate(8);
      buffer.clear();
      buffer.putLong(0, x);
      return buffer.array();
    }

    public static long bytesToLong(byte[] bytes) {
      ByteBuffer buffer = ByteBuffer.wrap(bytes, 0, 8);
      buffer.clear();
      // logger.debug("remaining: " + buffer.remaining());
      return buffer.getLong();
    }

    /**
     * create a byte array with random content.
     */
    public static byte[] randomBytes(int length) {
      ByteBuffer buffer = ByteBuffer.allocate(length);
      for (int i = 0; i < length; ++i) {
        buffer.put((byte) RandomUtils.nextInt());
      }

      return buffer.array();
    }
  }
}
