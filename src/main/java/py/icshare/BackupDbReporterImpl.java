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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.RequestResponseHelper;
import py.common.Utils;
import py.common.struct.EndPoint;
import py.icshare.authorization.ApiToAuthorize;
import py.icshare.authorization.PyResource;
import py.icshare.authorization.Role;
import py.icshare.iscsiaccessrule.Iscsi2AccessRuleRelationship;
import py.icshare.iscsiaccessrule.IscsiAccessRule;
import py.icshare.iscsiaccessrule.IscsiAccessRuleInformation;
import py.icshare.iscsiaccessrule.IscsiRuleRelationshipInformation;
import py.icshare.qos.MigrationRule;
import py.icshare.qos.MigrationRuleInformation;
import py.icshare.qos.RebalanceRuleInformation;
import py.informationcenter.StoragePool;
import py.informationcenter.StoragePoolInformation;
import py.instance.Group;
import py.io.qos.IoLimitation;
import py.thrift.share.AccountMetadataBackupThrift;
import py.thrift.share.ApiToAuthorizeThrift;
import py.thrift.share.CapacityRecordThrift;
import py.thrift.share.DomainThrift;
import py.thrift.share.GroupThrift;
import py.thrift.share.IoLimitationThrift;
import py.thrift.share.IscsiAccessRuleThrift;
import py.thrift.share.IscsiRuleRelationshipThrift;
import py.thrift.share.MigrationRuleThrift;
import py.thrift.share.RebalanceRulethrift;
import py.thrift.share.ReportDbRequestThrift;
import py.thrift.share.ReportDbResponseThrift;
import py.thrift.share.ResourceThrift;
import py.thrift.share.RoleThrift;
import py.thrift.share.StoragePoolThrift;
import py.thrift.share.VolumeAccessRuleThrift;
import py.thrift.share.VolumeRecycleInformationThrift;
import py.thrift.share.VolumeRuleRelationshipThrift;

public class BackupDbReporterImpl implements BackupDbReporter {
  private static final Logger logger = LoggerFactory.getLogger(BackupDbReporterImpl.class);
  private static final String sequenceIdFileName = "sequenceId_File";
  List<VolumeRecycleInformation> volumeRecycleInformationList;
  private Long sequenceId;
  private String backupDbPath;
  private ObjectMapper writeObjectMapper;
  private ObjectMapper readObjectMapper;
  private OutputStream externalOutputStream;
  private InputStream externalInputStream;
  private byte[] newline;
  // store all database tables in memory
  private List<DomainInformation> domainList;
  private List<StoragePoolInformation> storagePoolList;
  private List<VolumeRuleRelationshipInformation> volume2RuleList;
  private List<AccessRuleInformation> accessRuleList;
  private List<CapacityRecordInformation> capacityRecordList;
  private List<ApiToAuthorize> apiList;
  private List<PyResource> resourceList;
  private List<Role> roleList;
  private List<AccountMetadata> accountList;
  private List<IoLimitation> ioLimitationList;
  private List<MigrationRuleInformation> migrationRuleInformationList;
  private List<RebalanceRuleInformation> rebalanceRuleInformationList;

  private List<IscsiRuleRelationshipInformation> iscsi2RuleList;
  private List<IscsiAccessRuleInformation> iscsiAccessRuleList;

  public BackupDbReporterImpl(String backupDbPath) {
    this.backupDbPath = backupDbPath;
    this.domainList = new ArrayList<>();
    this.storagePoolList = new ArrayList<>();
    this.volume2RuleList = new ArrayList<>();
    this.accessRuleList = new ArrayList<>();
    this.capacityRecordList = new ArrayList<>();
    this.iscsi2RuleList = new ArrayList<>();
    this.iscsiAccessRuleList = new ArrayList<>();
    apiList = new ArrayList<>();
    resourceList = new ArrayList<>();
    roleList = new ArrayList<>();
    accountList = new ArrayList<>();

    volumeRecycleInformationList = new ArrayList<>();

    ioLimitationList = new ArrayList<>();
    migrationRuleInformationList = new ArrayList<>();
    rebalanceRuleInformationList = new ArrayList<>();

    try {
      writeObjectMapper = new ObjectMapper();
      writeObjectMapper.getFactory().disable(JsonGenerator.Feature.AUTO_CLOSE_TARGET);

      readObjectMapper = new ObjectMapper();
      readObjectMapper.getFactory().disable(JsonParser.Feature.AUTO_CLOSE_SOURCE);
      newline = System.getProperty("line.separator").getBytes();
    } catch (Exception e) {
      logger.error("no way we can catch this.", e);
      throw new RuntimeException(e);
    }

    // load sequence id from file
    Path sequenceIdPath = buildPathByFileName(sequenceIdFileName);
    try {
      if (!Files.exists(sequenceIdPath.getParent())) {
        Files.createDirectories(sequenceIdPath.getParent());
      }
      if (Files.exists(sequenceIdPath)) {
        // if exists, read sequence id from file
        List<Long> stringList = loadFromFile(sequenceIdFileName, Long.class);
        if (stringList.isEmpty()) {
          saveSequenceId2File(0L);
        } else {
          Validate.isTrue(stringList.size() == 1, "sequenceId is only one: %s", stringList);
          this.sequenceId = stringList.get(0);
        }
      } else {
        // if not exists, init sequence id and create sequence id file
        Files.createFile(sequenceIdPath);
        saveSequenceId2File(0L);
      }
    } catch (Exception e) {
      logger.error("file:{} process caught an exception", sequenceIdPath.toString(), e);
      throw new RuntimeException();
    }

    // load database tables from file
    try {
      loadDbInfo();
    } catch (Exception e) {
      logger.error("caught an exception when load database tables from file", e);
      throw new RuntimeException();
    }
  }

  @Override
  public ReportDbRequestThrift buildReportDbRequest(EndPoint endPoint, Group group, Long instanceId,
      boolean carryDbInfo) {
    ReportDbRequestThrift reportDbRequest = new ReportDbRequestThrift();
    reportDbRequest.setEndpoint(endPoint.toString());
    GroupThrift groupThrift = RequestResponseHelper.buildThriftGroupFrom(group);
    reportDbRequest.setGroup(groupThrift);
    reportDbRequest.setInstanceId(instanceId);

    // sequence id
    reportDbRequest.setSequenceId(sequenceId);

    if (!carryDbInfo) {
      return reportDbRequest;
    }

    // domain
    if (domainList != null && !domainList.isEmpty()) {
      List<DomainThrift> domainThriftList = new ArrayList<>();
      for (DomainInformation domainInfo : domainList) {
        Domain domain = domainInfo.toDomain();
        DomainThrift domainThrift = RequestResponseHelper.buildDomainThriftFrom(domain);
        domainThriftList.add(domainThrift);
      }
      reportDbRequest.setDomainThriftList(domainThriftList);
    }

    // storagePool
    if (storagePoolList != null && !storagePoolList.isEmpty()) {
      List<StoragePoolThrift> storagePoolThriftList = new ArrayList<>();
      for (StoragePoolInformation storagePoolInfo : storagePoolList) {
        StoragePool storagePool = storagePoolInfo.toStoragePool();
        StoragePoolThrift storagePoolThrift = RequestResponseHelper
            .buildThriftStoragePoolFrom(storagePool, null);
        storagePoolThriftList.add(storagePoolThrift);
      }
      reportDbRequest.setStoragePoolThriftList(storagePoolThriftList);
    }

    // volumeRuleRelationship
    if (volume2RuleList != null && !volume2RuleList.isEmpty()) {
      List<VolumeRuleRelationshipThrift> volume2RuleThriftList = new ArrayList<>();
      for (VolumeRuleRelationshipInformation volume2RuleInfo : volume2RuleList) {
        Volume2AccessRuleRelationship volume2Rule = volume2RuleInfo
            .toVolume2AccessRuleRelationship();
        VolumeRuleRelationshipThrift volume2RuleThrift = RequestResponseHelper
            .buildThriftVolumeRuleRelationship(volume2Rule);
        volume2RuleThriftList.add(volume2RuleThrift);
      }
      reportDbRequest.setVolume2RuleThriftList(volume2RuleThriftList);
    }

    // accessRule
    if (accessRuleList != null && !accessRuleList.isEmpty()) {
      List<VolumeAccessRuleThrift> accessRuleThriftList = new ArrayList<>();
      for (AccessRuleInformation accessRuleInfo : accessRuleList) {
        VolumeAccessRule accessRule = accessRuleInfo.toVolumeAccessRule();
        VolumeAccessRuleThrift accessRuleThrift = RequestResponseHelper
            .buildVolumeAccessRuleThriftFrom(accessRule);
        accessRuleThriftList.add(accessRuleThrift);
      }
      reportDbRequest.setAccessRuleThriftList(accessRuleThriftList);
    }

    // iscsiRuleRelationship
    if (iscsi2RuleList != null && !iscsi2RuleList.isEmpty()) {
      List<IscsiRuleRelationshipThrift> iscsi2RuleThriftList = new ArrayList<>();
      for (IscsiRuleRelationshipInformation iscsi2RuleInfo : iscsi2RuleList) {
        Iscsi2AccessRuleRelationship iscsi2Rule = iscsi2RuleInfo.toIscsi2AccessRuleRelationship();
        IscsiRuleRelationshipThrift iscsi2RuleThrift = RequestResponseHelper
            .buildThriftIscsiRuleRelationship(iscsi2Rule);
        iscsi2RuleThriftList.add(iscsi2RuleThrift);
      }
      reportDbRequest.setIscsi2RuleThriftList(iscsi2RuleThriftList);
    }

    // iscsiAccessRule
    if (iscsiAccessRuleList != null && !iscsiAccessRuleList.isEmpty()) {
      List<IscsiAccessRuleThrift> iscsiAccessRuleThriftList = new ArrayList<>();
      for (IscsiAccessRuleInformation iscsiAccessRuleInfo : iscsiAccessRuleList) {
        IscsiAccessRule iscsiAccessRule = iscsiAccessRuleInfo.toIscsiAccessRule();
        IscsiAccessRuleThrift iscsiAccessRuleThrift = RequestResponseHelper
            .buildIscsiAccessRuleThriftFrom(iscsiAccessRule);
        iscsiAccessRuleThriftList.add(iscsiAccessRuleThrift);
      }
      reportDbRequest.setIscsiAccessRuleThriftList(iscsiAccessRuleThriftList);
    }

    // capacity record
    if (capacityRecordList != null && !capacityRecordList.isEmpty()) {
      List<CapacityRecordThrift> capacityRecordThriftList = new ArrayList<>();
      for (CapacityRecordInformation capacityRecordInfo : capacityRecordList) {
        CapacityRecord capacityRecord = capacityRecordInfo.toCapacityRecord();
        CapacityRecordThrift capacityRecordThrift = RequestResponseHelper
            .buildThriftCapacityRecordFrom(capacityRecord);
        capacityRecordThriftList.add(capacityRecordThrift);
      }
      reportDbRequest.setCapacityRecordThriftList(capacityRecordThriftList);
    }

    if (null != apiList && !apiList.isEmpty()) {
      List<ApiToAuthorizeThrift> apiToAuthorizeThriftList = new ArrayList<>();
      for (ApiToAuthorize api : apiList) {
        apiToAuthorizeThriftList.add(RequestResponseHelper.buildApiToAuthorizeThrift(api));
      }
      reportDbRequest.setApiThriftList(apiToAuthorizeThriftList);
    }

    if (null != resourceList && !resourceList.isEmpty()) {
      List<ResourceThrift> resourceThriftList = new ArrayList<>();
      for (PyResource resource : resourceList) {
        resourceThriftList.add(RequestResponseHelper.buildResourceThrift(resource));
      }
      reportDbRequest.setResourceThriftList(resourceThriftList);
    }

    if (null != roleList && !roleList.isEmpty()) {
      List<RoleThrift> roleThriftList = new ArrayList<>();
      for (Role role : roleList) {
        roleThriftList.add(RequestResponseHelper.buildRoleThrift(role));
      }
      reportDbRequest.setRoleThriftList(roleThriftList);
    }

    if (null != accountList && !accountList.isEmpty()) {
      List<AccountMetadataBackupThrift> accountMetadataBackupThriftList = new ArrayList<>();
      for (AccountMetadata accountMetadata : accountList) {
        accountMetadataBackupThriftList.add(
            RequestResponseHelper.buildAccountMetadataBackupThrift(accountMetadata));
      }
      reportDbRequest.setAccountMetadataBackupThriftList(accountMetadataBackupThriftList);
    }

    //ioLimitation IoLimitationThrift
    if (null != ioLimitationList && !ioLimitationList.isEmpty()) {
      List<IoLimitationThrift> ioLimitationThriftList = new ArrayList<>();
      for (IoLimitation ioLimitation : ioLimitationList) {
        ioLimitationThriftList.add(
            RequestResponseHelper.buildThriftIoLimitationFrom(ioLimitation));
      }
      reportDbRequest.setIoLimitationThriftList(ioLimitationThriftList);
    }

    // migrationRule
    if (null != migrationRuleInformationList && !migrationRuleInformationList.isEmpty()) {
      List<MigrationRuleThrift> migrationSpeedRuleThriftList = new ArrayList<>();
      for (MigrationRuleInformation migrationRuleInformation : migrationRuleInformationList) {
        migrationSpeedRuleThriftList.add(
            RequestResponseHelper
                .buildMigrationRuleThriftFrom(new MigrationRule(migrationRuleInformation)));
      }
      reportDbRequest.setMigrationRuleThriftList(migrationSpeedRuleThriftList);
    }

    //rebalanceRuleStore
    if (null != rebalanceRuleInformationList && !rebalanceRuleInformationList.isEmpty()) {
      List<RebalanceRulethrift> rebalanceRuleThriftList = new ArrayList<>();
      for (RebalanceRuleInformation rebalanceRuleInformation : rebalanceRuleInformationList) {
        rebalanceRuleThriftList.add(
            RequestResponseHelper.convertRebalanceRule2RebalanceRuleThrift(
                rebalanceRuleInformation.toRebalanceRule()));
      }
      reportDbRequest.setRebalanceRuleThriftList(rebalanceRuleThriftList);
    }

    // volume_recycle  VolumeRecycleInformationThrift
    if (volumeRecycleInformationList != null && !volumeRecycleInformationList.isEmpty()) {
      List<VolumeRecycleInformationThrift> volumeRecycleInformationThrifts = new ArrayList<>();

      for (VolumeRecycleInformation volumeRecycleInformation : volumeRecycleInformationList) {
        volumeRecycleInformationThrifts.add(RequestResponseHelper
            .buildThriftVolumeRecycleInformationFrom(volumeRecycleInformation));
      }
      reportDbRequest.setVolumeRecycleInformationThriftList(volumeRecycleInformationThrifts);
    }

    return reportDbRequest;
  }

  private void saveSequenceId2File(Long sequenceId) {
    try {
      List<Long> sequenceIdList = new ArrayList<>();
      sequenceIdList.add(sequenceId);
      writeObjectToFile(sequenceIdFileName, sequenceIdList);
      this.sequenceId = sequenceId;
    } catch (Exception e) {
      logger.error("failed to save sequence id to file", e);
    }
  }

  @Override
  public synchronized void processRsp(ReportDbResponseThrift response) {
    if (response == null) {
      logger.warn("DB response is null");
      return;
    }

    logger.debug("report DB response:{}", response);
    if (!response.isSetDomainThriftList() && !response.isSetVolume2RuleThriftList()
        && !response.isSetAccessRuleThriftList() && !response.isSetStoragePoolThriftList()
        && !response.isSetCapacityRecordThriftList()) {
      logger.debug("no need to save any database tables, current response sequenceId:{}",
          response.getSequenceId());
      return;
    }
    // process sequence id
    Long sequenceId = response.getSequenceId();

    if (sequenceId < this.sequenceId) {
      return;
    }

    // save sequence id to file first
    saveSequenceId2File(sequenceId);

    // domain
    List<DomainThrift> domainThriftList = response.getDomainThriftList();
    if (domainThriftList != null && !domainThriftList.isEmpty()) {
      List<DomainInformation> domainInfoList = new ArrayList<>();
      try {
        for (DomainThrift domainThrift : domainThriftList) {
          Domain domain = RequestResponseHelper.buildDomainFrom(domainThrift);
          DomainInformation domainInfo = domain.toDomainInformation();
          domainInfoList.add(domainInfo);
        }
        // compare two list,if has any update, save to file and memory
        if (domainInfoList.size() != this.domainList.size()
            || !Utils.compareTwoList(domainInfoList, this.domainList)) {
          // update to file
          writeObjectToFile(DbTableName.Domain.name(), domainInfoList);
          // update memory
          this.domainList = domainInfoList;
        }
      } catch (Exception e) {
        logger
            .error("can not update newest domains to file, current domains:{}", domainInfoList, e);
      }
    }

    // storagePool
    List<StoragePoolThrift> storagePoolThriftList = response.getStoragePoolThriftList();
    if (storagePoolThriftList != null && !storagePoolThriftList.isEmpty()) {
      List<StoragePoolInformation> storagePoolInfoList = new ArrayList<>();
      try {
        for (StoragePoolThrift storagePoolThrift : storagePoolThriftList) {
          StoragePool storagePool = RequestResponseHelper
              .buildStoragePoolFromThrift(storagePoolThrift);
          StoragePoolInformation storagePoolInfo = storagePool.toStoragePoolInformation();
          storagePoolInfoList.add(storagePoolInfo);
        }
        // compare two list,if has any update, save to file and memory
        if (storagePoolInfoList.size() != this.storagePoolList.size()
            || !Utils.compareTwoList(storagePoolInfoList, this.storagePoolList)) {
          // update to file
          writeObjectToFile(DbTableName.StoragePool.name(), storagePoolInfoList);
          // update memory
          this.storagePoolList = storagePoolInfoList;
        }
      } catch (Exception e) {
        logger.error("can not update newest storagePools to file, current storagePools:{}",
            storagePoolInfoList,
            e);
      }
    }

    // volumeRuleRelationship
    List<VolumeRuleRelationshipThrift> volume2RuleThriftList = response.getVolume2RuleThriftList();
    if (volume2RuleThriftList != null && !volume2RuleThriftList.isEmpty()) {
      List<VolumeRuleRelationshipInformation> volume2RuleInfoList = new ArrayList<>();
      try {
        for (VolumeRuleRelationshipThrift volume2RuleThrift : volume2RuleThriftList) {
          Volume2AccessRuleRelationship volume2Rule = RequestResponseHelper
              .buildVolume2AccessRuleRelationshipFromThrift(volume2RuleThrift);
          volume2RuleInfoList.add(volume2Rule.toVolumeRuleRelationshipInformation());
        }
        // compare two list,if has any update, save to file and memory
        if (volume2RuleInfoList.size() != this.volume2RuleList.size()
            || !Utils.compareTwoList(volume2RuleInfoList, this.volume2RuleList)) {
          // update to file
          writeObjectToFile(DbTableName.Volume2RuleRelate.name(), volume2RuleInfoList);
          // update memory
          this.volume2RuleList = volume2RuleInfoList;
        }
      } catch (Exception e) {
        logger.error("can not update newest volume2Rules to file, current volume2Rules:{}",
            volume2RuleInfoList,
            e);
      }
    }

    // accessRule
    List<VolumeAccessRuleThrift> accessRuleThriftList = response.getAccessRuleThriftList();
    if (accessRuleThriftList != null && !accessRuleThriftList.isEmpty()) {
      List<AccessRuleInformation> accessRuleInfoList = new ArrayList<>();
      try {
        for (VolumeAccessRuleThrift accessRuleThrift : accessRuleThriftList) {
          VolumeAccessRule accessRule = RequestResponseHelper
              .buildVolumeAccessRuleFrom(accessRuleThrift);
          accessRuleInfoList.add(accessRule.toAccessRuleInformation());
        }
        // compare two list,if has any update, save to file and memory
        if (accessRuleInfoList.size() != this.accessRuleList.size()
            || !Utils.compareTwoList(accessRuleInfoList, this.accessRuleList)) {
          // update to file
          writeObjectToFile(DbTableName.AccessRule.name(), accessRuleInfoList);
          // update memory
          this.accessRuleList = accessRuleInfoList;
        }
      } catch (Exception e) {
        logger.error("can not update newest accessRules to file, current accessRules:{}",
            accessRuleInfoList,
            e);
      }
    }

    // iscsiRuleRelationship
    List<IscsiRuleRelationshipThrift> iscsi2RuleThriftList = response.getIscsi2RuleThriftList();
    if (iscsi2RuleThriftList != null && !iscsi2RuleThriftList.isEmpty()) {
      List<IscsiRuleRelationshipInformation> iscsi2RuleInfoList = new ArrayList<>();
      try {
        for (IscsiRuleRelationshipThrift iscsi2RuleThrift : iscsi2RuleThriftList) {
          Iscsi2AccessRuleRelationship iscsi2Rule = RequestResponseHelper
              .buildIscsi2AccessRuleRelationshipFromThrift(iscsi2RuleThrift);
          iscsi2RuleInfoList.add(iscsi2Rule.toIscsiRuleRelationshipInformation());
        }
        // compare two list,if has any update, save to file and memory
        if (iscsi2RuleInfoList.size() != this.iscsi2RuleList.size()
            || !Utils.compareTwoList(iscsi2RuleInfoList, this.iscsi2RuleList)) {
          // update to file
          writeObjectToFile(DbTableName.Iscsi2RuleRelate.name(), iscsi2RuleInfoList);
          // update memory
          this.iscsi2RuleList = iscsi2RuleInfoList;
        }
      } catch (Exception e) {
        logger.error("can not update newest iscsi2Rules to file, current iscsi2Rules:{}",
            iscsi2RuleInfoList,
            e);
      }
    }

    // iscsiAccessRule
    List<IscsiAccessRuleThrift> iscsiAccessRuleThriftList = response.getIscsiAccessRuleThriftList();
    if (iscsiAccessRuleThriftList != null && !iscsiAccessRuleThriftList.isEmpty()) {
      List<IscsiAccessRuleInformation> iscsiAccessRuleInfoList = new ArrayList<>();
      try {
        for (IscsiAccessRuleThrift iscsiAccessRuleThrift : iscsiAccessRuleThriftList) {
          IscsiAccessRule accessRule = RequestResponseHelper
              .buildIscsiAccessRuleFrom(iscsiAccessRuleThrift);
          iscsiAccessRuleInfoList.add(accessRule.toIscsiAccessRuleInformation());
        }
        // compare two list,if has any update, save to file and memory
        if (iscsiAccessRuleInfoList.size() != this.iscsiAccessRuleList.size()
            || !Utils.compareTwoList(iscsiAccessRuleInfoList, this.iscsiAccessRuleList)) {
          // update to file
          writeObjectToFile(DbTableName.IscsiAccessRule.name(), iscsiAccessRuleInfoList);
          // update memory
          this.iscsiAccessRuleList = iscsiAccessRuleInfoList;
        }
      } catch (Exception e) {
        logger.error("can not update newest iscsiAccessRules to file, current iscsiAccessRules:{}",
            iscsiAccessRuleInfoList,
            e);
      }
    }

    // capacity record
    List<CapacityRecordThrift> capacityRecordThriftList = response.getCapacityRecordThriftList();
    if (capacityRecordThriftList != null && !capacityRecordThriftList.isEmpty()) {
      List<CapacityRecordInformation> capacityRecordInfoList = new ArrayList<>();
      try {
        for (CapacityRecordThrift capacityRecordThrift : capacityRecordThriftList) {
          CapacityRecord capacityRecord = RequestResponseHelper
              .buildCapacityRecordFrom(capacityRecordThrift);
          capacityRecordInfoList.add(capacityRecord.toCapacityRecordInformation());
        }
        // compare two list,if has any update, save to file and memory
        if (capacityRecordInfoList.size() != this.capacityRecordList.size()
            || !Utils.compareTwoList(capacityRecordInfoList, this.capacityRecordList)) {
          // update to file
          writeObjectToFile(DbTableName.CapacityRecord.name(), capacityRecordInfoList);
          // update memory
          this.capacityRecordList = capacityRecordInfoList;
        }
      } catch (Exception e) {
        logger.error("can not update newest capacityRecords to file, current capacityRecords:{}",
            capacityRecordInfoList, e);
      }
    }

    // api
    List<ApiToAuthorizeThrift> apiThrifts = response.getApiThriftList();
    if (null != apiThrifts && !apiThrifts.isEmpty()) {
      try {
        List<ApiToAuthorize> apiToAuthorizes = new ArrayList<>();
        for (ApiToAuthorizeThrift apiThrift : apiThrifts) {
          apiToAuthorizes.add(RequestResponseHelper.buildApiToAuthorizeFrom(apiThrift));
        }
        if (apiToAuthorizes.size() != apiList.size() || !Utils
            .compareTwoList(apiToAuthorizes, apiList)) {
          writeObjectToFile(DbTableName.API.name(), apiToAuthorizes);
          apiList = apiToAuthorizes;
        }
      } catch (Exception e) {
        logger.error("can not update newest apis to file, current apis: {}",
            apiList, e);
      }
    }

    // resources
    List<ResourceThrift> resourceThrifts = response.getResourceThriftList();
    if (null != resourceThrifts && !resourceThrifts.isEmpty()) {
      try {
        List<PyResource> resources = new ArrayList<>();
        for (ResourceThrift resourceThrift : resourceThrifts) {
          resources.add(RequestResponseHelper.buildResourceFrom(resourceThrift));
        }
        if (resources.size() != resourceList.size() || !Utils
            .compareTwoList(resources, resourceList)) {
          writeObjectToFile(DbTableName.RESOURCE.name(), resources);
          resourceList = resources;
        }
      } catch (Exception e) {
        logger.error("can not update newest resources to file, current resources: {}",
            resourceList, e);
      }
    }

    // roles
    List<RoleThrift> roleThrifts = response.getRoleThriftList();
    if (null != roleThrifts && !roleThrifts.isEmpty()) {
      try {
        List<Role> roles = new ArrayList<>();
        for (RoleThrift roleThrift : roleThrifts) {
          roles.add(RequestResponseHelper.buildRoleFrom(roleThrift));
        }
        if (roles.size() != roleList.size() || !Utils.compareTwoList(roles, roleList)) {
          writeObjectToFile(DbTableName.ROLE.name(), roles);
          roleList = roles;
        }
      } catch (Exception e) {
        logger.error("can not update newest roles to file, current roles: {}",
            roleList, e);
      }
    }

    // Account
    List<AccountMetadataBackupThrift> accountThrifts = response
        .getAccountMetadataBackupThriftList();
    if (null != accountThrifts && !accountThrifts.isEmpty()) {
      try {
        List<AccountMetadata> accounts = new ArrayList<>();
        for (AccountMetadataBackupThrift accountMetadataBackupThrift : accountThrifts) {
          accounts.add(
              RequestResponseHelper.buildAccountMetadataBackupFrom(accountMetadataBackupThrift));
        }
        if (accounts.size() != accountList.size() || !Utils.compareTwoList(accounts, accountList)) {
          writeObjectToFile(DbTableName.ACCOUNT.name(), accounts);
          accountList = accounts;
        }
      } catch (Exception e) {
        logger.error("can not update newest accounts to file, current accounts: {}",
            accountList, e);
      }
    }

    // ioLimitation
    List<IoLimitationThrift> ioLimitationThriftList = response.getIoLimitationThriftList();
    if (ioLimitationThriftList != null && !ioLimitationThriftList.isEmpty()) {
      try {
        List<IoLimitation> iolimitions = new ArrayList<>();
        for (IoLimitationThrift ioLimitationThrift : ioLimitationThriftList) {
          iolimitions.add(RequestResponseHelper.buildIoLimitationFrom(ioLimitationThrift));
        }
        if (iolimitions.size() != ioLimitationList.size() || !Utils
            .compareTwoList(iolimitions, ioLimitationList)) {
          writeObjectToFile(DbTableName.IoLimitation.name(), iolimitions);
          ioLimitationList = iolimitions;
        }
      } catch (Exception e) {
        logger.error("can not update newest iolimition to file, current iolimitions: {}",
            ioLimitationList, e);
      }
    }

    // migrationRule
    List<MigrationRuleThrift> migrationSpeedRuleThriftList = response.getMigrationSpeedThriftList();
    if (migrationSpeedRuleThriftList != null && !migrationSpeedRuleThriftList.isEmpty()) {
      try {
        List<MigrationRuleInformation> migrationRuleInformations = new ArrayList<>();
        for (MigrationRuleThrift migrationSpeedRuleThrift : migrationSpeedRuleThriftList) {
          MigrationRule migrationSpeed = RequestResponseHelper
              .buildMigrationRuleFrom(migrationSpeedRuleThrift);
          migrationRuleInformations.add(migrationSpeed.toMigrationRuleInformation());
        }
        if (migrationRuleInformations.size() != migrationRuleInformationList.size()
            || !Utils.compareTwoList(migrationRuleInformations, migrationRuleInformationList)) {
          writeObjectToFile(DbTableName.MigrationRule.name(), migrationRuleInformations);
          migrationRuleInformationList = migrationRuleInformations;
        }
      } catch (Exception e) {
        logger.error("can not update newest migrationRule to file, current migrationRules: {}",
            migrationRuleInformationList, e);
      }
    }

    //rebalanceRuleStore
    List<RebalanceRulethrift> rebalanceRuleThriftList = response.getRebalanceRuleThriftList();
    if (rebalanceRuleThriftList != null && !rebalanceRuleThriftList.isEmpty()) {
      try {
        List<RebalanceRuleInformation> rebalanceRuleInformations = new ArrayList<>();
        for (RebalanceRulethrift rebalanceRuleThrift : rebalanceRuleThriftList) {
          rebalanceRuleInformations.add(
              RequestResponseHelper.convertRebalanceRuleThrift2RebalanceRule(rebalanceRuleThrift)
                  .toRebalanceRuleInformation());
        }
        if (rebalanceRuleInformations.size() != rebalanceRuleInformationList.size()
            || !Utils.compareTwoList(rebalanceRuleInformations, rebalanceRuleInformationList)) {
          writeObjectToFile(DbTableName.RebalanceRule.name(), rebalanceRuleInformations);
          rebalanceRuleInformationList = rebalanceRuleInformations;
        }
      } catch (Exception e) {
        logger.error(
            "can not update newest rebalanceRuleS to file, current rebalanceRuleInformations: {}",
            rebalanceRuleInformationList, e);
      }
    }

    // volume_recycle  VolumeRecycleInformationThrift
    List<VolumeRecycleInformationThrift> volumeRecycleInformationThrifts = response
        .getVolumeRecycleInformationThriftList();
    if (volumeRecycleInformationThrifts != null && !volumeRecycleInformationThrifts.isEmpty()) {
      try {
        List<VolumeRecycleInformation> volumeRecycleInformationListGet = new ArrayList<>();
        for (VolumeRecycleInformationThrift
            volumeRecycleInformationThrift : volumeRecycleInformationThrifts) {
          volumeRecycleInformationListGet.add(RequestResponseHelper
              .buildVolumeRecycleInformationFrom(volumeRecycleInformationThrift));
        }

        if (volumeRecycleInformationListGet.size() != volumeRecycleInformationList.size()
            || !Utils
            .compareTwoList(volumeRecycleInformationListGet, volumeRecycleInformationList)) {
          writeObjectToFile(DbTableName.VolumeRecycleInformation.name(),
              volumeRecycleInformationListGet);
          volumeRecycleInformationList = volumeRecycleInformationListGet;
        }
      } catch (Exception e) {
        logger.error(
            "can not update newest VolumeRecycleInformation to file, current "
                + "VolumeRecycleInformation: {}",
            volumeRecycleInformationList, e);
      }
    }
  }

  @Override
  public void loadDbInfo() throws Exception {
    List<Long> sequenceIdList = loadFromFile(sequenceIdFileName, Long.class);
    if (sequenceIdList.size() != 0) {
      Validate.isTrue(sequenceIdList.size() == 1, "sequenceIdList: %s", sequenceIdList);
      sequenceId = sequenceIdList.get(0);
    }
    domainList = loadFromFile(DbTableName.Domain.name(), DomainInformation.class);
    storagePoolList = loadFromFile(DbTableName.StoragePool.name(), StoragePoolInformation.class);
    volume2RuleList = loadFromFile(DbTableName.Volume2RuleRelate.name(),
        VolumeRuleRelationshipInformation.class);
    accessRuleList = loadFromFile(DbTableName.AccessRule.name(), AccessRuleInformation.class);
    capacityRecordList = loadFromFile(DbTableName.CapacityRecord.name(),
        CapacityRecordInformation.class);
    apiList = loadFromFile(DbTableName.API.name(), ApiToAuthorize.class);
    resourceList = loadFromFile(DbTableName.RESOURCE.name(), PyResource.class);
    roleList = loadFromFile(DbTableName.ROLE.name(), Role.class);
    accountList = loadFromFile(DbTableName.ACCOUNT.name(), AccountMetadata.class);
    iscsi2RuleList = loadFromFile(DbTableName.Iscsi2RuleRelate.name(),
        IscsiRuleRelationshipInformation.class);
    iscsiAccessRuleList = loadFromFile(DbTableName.IscsiAccessRule.name(),
        IscsiAccessRuleInformation.class);

    volumeRecycleInformationList = loadFromFile(DbTableName.VolumeRecycleInformation.name(),
        VolumeRecycleInformation.class);
  }

  private <T> void writeObjectToFile(String fileName, List<T> objectList) throws Exception {
    Path filePath = buildPathByFileName(fileName);
    try {
      // open FD, no need append option for file
      this.externalOutputStream = new FileOutputStream(filePath.toFile(), false);
      for (T obj : objectList) {
        logger.debug("write file:{}, content:{}", fileName, obj);
        writeObjectMapper.writeValue(this.externalOutputStream, obj);
        // write line.separator
        externalOutputStream.write(newline);
      }
    } catch (Exception e) {
      logger.error("failed to write:{} to file:{}", objectList, fileName, e);
    } finally {
      if (this.externalOutputStream != null) {
        this.externalOutputStream.close();
      }
    }
  }

  private <T> List<T> loadFromFile(String fileName, Class<T> object) throws Exception {
    Path filePath = buildPathByFileName(fileName);
    // determine if file exist
    if (!Files.exists(filePath)) {
      return new ArrayList<>();
    }
    List<T> objectList = new ArrayList<>();
    try {
      this.externalInputStream = new FileInputStream(filePath.toFile());
      BufferedReader br = new BufferedReader(new InputStreamReader(this.externalInputStream));
      String line;
      while ((line = br.readLine()) != null) {
        T element = readObjectMapper.readValue(line, object);
        objectList.add(element);
      }
    } catch (Exception e) {
      logger.error("failed to load info from file:{}", fileName, e);
    } finally {
      if (this.externalInputStream != null) {
        this.externalInputStream.close();
      }
    }
    return objectList;
  }

  private Path buildPathByFileName(String fileName) {
    Path pathToLogFile = FileSystems.getDefault().getPath(backupDbPath, fileName);
    return pathToLogFile;
  }

  // add getters and setters for junit test
  public Long getSequenceId() {
    return sequenceId;
  }

  public void setSequenceId(Long sequenceId) {
    this.sequenceId = sequenceId;
  }

  public List<DomainInformation> getDomainList() {
    return domainList;
  }

  public void setDomainList(List<DomainInformation> domainList) {
    this.domainList = domainList;
  }

  public List<StoragePoolInformation> getStoragePoolList() {
    return storagePoolList;
  }

  public void setStoragePoolList(List<StoragePoolInformation> storagePoolList) {
    this.storagePoolList = storagePoolList;
  }

  public List<VolumeRuleRelationshipInformation> getVolume2RuleList() {
    return volume2RuleList;
  }

  public void setVolume2RuleList(List<VolumeRuleRelationshipInformation> volume2RuleList) {
    this.volume2RuleList = volume2RuleList;
  }

  public List<AccessRuleInformation> getAccessRuleList() {
    return accessRuleList;
  }

  public void setAccessRuleList(List<AccessRuleInformation> accessRuleList) {
    this.accessRuleList = accessRuleList;
  }

  public List<IscsiRuleRelationshipInformation> getIscsi2RuleList() {
    return iscsi2RuleList;
  }

  public void setIscsi2RuleList(List<IscsiRuleRelationshipInformation> iscsi2RuleList) {
    this.iscsi2RuleList = iscsi2RuleList;
  }

  public List<IscsiAccessRuleInformation> getIscsiAccessRuleList() {
    return iscsiAccessRuleList;
  }

  public void setIscsiAccessRuleList(List<IscsiAccessRuleInformation> iscsiAccessRuleList) {
    this.iscsiAccessRuleList = iscsiAccessRuleList;
  }

  public List<CapacityRecordInformation> getCapacityRecordList() {
    return capacityRecordList;
  }

  public void setCapacityRecordList(List<CapacityRecordInformation> capacityRecordList) {
    this.capacityRecordList = capacityRecordList;
  }

  public List<VolumeRecycleInformation> getVolumeRecycleInformationList() {
    return volumeRecycleInformationList;
  }

  public void setVolumeRecycleInformationList(
      List<VolumeRecycleInformation> volumeRecycleInformationList) {
    this.volumeRecycleInformationList = volumeRecycleInformationList;
  }

}
