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

import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.icshare.authorization.ApiToAuthorize;
import py.icshare.authorization.PyResource;
import py.icshare.authorization.Role;
import py.icshare.iscsiaccessrule.Iscsi2AccessRuleRelationship;
import py.icshare.iscsiaccessrule.IscsiAccessRule;
import py.icshare.iscsiaccessrule.IscsiAccessRuleInformation;
import py.icshare.iscsiaccessrule.IscsiRuleRelationshipInformation;
import py.icshare.qos.MigrationRuleInformation;
import py.icshare.qos.RebalanceRuleInformation;
import py.informationcenter.StoragePoolInformationDb;
import py.io.qos.IoLimitation;
import py.thrift.share.AccountMetadataThrift;
import py.thrift.share.ApiToAuthorizeThrift;
import py.thrift.share.CapacityRecordThrift;
import py.thrift.share.DomainThrift;
import py.thrift.share.IoLimitationThrift;
import py.thrift.share.IscsiAccessRuleThrift;
import py.thrift.share.IscsiRuleRelationshipThrift;
import py.thrift.share.MigrationRuleThrift;
import py.thrift.share.ResourceThrift;
import py.thrift.share.RoleThrift;
import py.thrift.share.StoragePoolThrift;
import py.thrift.share.VolumeAccessRuleThrift;
import py.thrift.share.VolumeRecycleInformationThrift;
import py.thrift.share.VolumeRuleRelationshipThrift;

/**
 * just list database table names need backup.
 *
 */
public enum DbTableName {
  Domain(1, Domain.class, DomainInformationDb.class, DomainThrift.class),

  StoragePool(2, py.informationcenter.StoragePool.class, StoragePoolInformationDb.class,
      StoragePoolThrift.class),

  Volume2RuleRelate(3, Volume2AccessRuleRelationship.class, VolumeRuleRelationshipInformation.class,
      VolumeRuleRelationshipThrift.class),

  AccessRule(4, VolumeAccessRule.class, AccessRuleInformation.class, VolumeAccessRuleThrift.class),

  CapacityRecord(5, CapacityRecord.class, CapacityRecordInformation.class,
      CapacityRecordThrift.class),

  API(8, ApiToAuthorize.class, ApiToAuthorize.class, ApiToAuthorizeThrift.class),

  RESOURCE(9, PyResource.class, PyResource.class, ResourceThrift.class),

  ROLE(10, Role.class, Role.class, RoleThrift.class),

  ACCOUNT(11, AccountMetadata.class, AccountMetadata.class, AccountMetadataThrift.class),

  Iscsi2RuleRelate(12, Iscsi2AccessRuleRelationship.class, IscsiRuleRelationshipInformation.class,
      IscsiRuleRelationshipThrift.class),

  IscsiAccessRule(13, IscsiAccessRule.class, IscsiAccessRuleInformation.class,
      IscsiAccessRuleThrift.class),

  VolumeRecycleInformation(17, VolumeRecycleInformation.class, VolumeRecycleInformation.class,
      VolumeRecycleInformationThrift.class),

  IoLimitation(19, IoLimitation.class, IoLimitation.class, IoLimitationThrift.class),

  MigrationRule(20, MigrationRuleInformation.class, MigrationRuleInformation.class,
      MigrationRuleThrift.class),

  RebalanceRule(21, RebalanceRuleInformation.class, RebalanceRuleInformation.class,
      IoLimitationThrift.class);

  private static final Logger logger = LoggerFactory.getLogger(DbTableName.class);
  private final int value;

  private Class<?> tableClass;
  private Class<?> tableInformationClass;
  private Class<?> tableThriftClass;

  DbTableName(int value, Class<?> tableClass, Class<?> tableInformationClass,
      Class<?> tableThriftClass) {
    this.value = value;
    this.setTableClass(tableClass);
    this.setTableInformationClass(tableInformationClass);
    this.setTableThriftClass(tableThriftClass);
  }

  public static DbTableName findByName(String name) {
    switch (name) {
      case "Domain":
        return Domain;
      case "StoragePool":
        return StoragePool;
      case "API":
        return API;
      case "RESOURCE":
        return RESOURCE;
      case "ROLE":
        return ROLE;
      case "ACCOUNT":
        return ACCOUNT;
      case "VolumeRecycleInformation":
        return VolumeRecycleInformation;
      case "IoLimitation":
        return IoLimitation;
      case "MigrationRule":
        return MigrationRule;
      case "RebalanceRule":
        return RebalanceRule;

      default:
        logger.error("can not find value by name:{}", name);
        return null;
    }
  }

  public static DbTableName findByValue(int value) {
    switch (value) {
      case 1:
        return Domain;
      case 2:
        return StoragePool;
      case 8:
        return API;
      case 9:
        return RESOURCE;
      case 10:
        return ROLE;
      case 11:
        return ACCOUNT;
      case 17:
        return VolumeRecycleInformation;
      case 19:
        return IoLimitation;
      case 20:
        return MigrationRule;
      case 21:
        return RebalanceRule;
      default:
        logger.error("can not find string by value:{}", value);
        return null;
    }
  }

  public static List<DbTableName> getAllDbTableName() {
    List<DbTableName> allDbTableName = new ArrayList<>();
    allDbTableName.add(Domain);
    allDbTableName.add(StoragePool);
    allDbTableName.add(Volume2RuleRelate);
    allDbTableName.add(AccessRule);
    allDbTableName.add(CapacityRecord);
    allDbTableName.add(API);
    allDbTableName.add(RESOURCE);
    allDbTableName.add(ROLE);
    allDbTableName.add(ACCOUNT);
    allDbTableName.add(Iscsi2RuleRelate);
    allDbTableName.add(IscsiAccessRule);

    allDbTableName.add(VolumeRecycleInformation);

    allDbTableName.add(IoLimitation);
    allDbTableName.add(MigrationRule);
    allDbTableName.add(RebalanceRule);
    return allDbTableName;
  }

  public int getValue() {
    return value;
  }

  public Class<?> getTableClass() {
    return tableClass;
  }

  public void setTableClass(Class<?> tableClass) {
    this.tableClass = tableClass;
  }

  public Class<?> getTableInformationClass() {
    return tableInformationClass;
  }

  public void setTableInformationClass(Class<?> tableInformationClass) {
    this.tableInformationClass = tableInformationClass;
  }

  public Class<?> getTableThriftClass() {
    return tableThriftClass;
  }

  public void setTableThriftClass(Class<?> tableThriftClass) {
    this.tableThriftClass = tableThriftClass;
  }

}
