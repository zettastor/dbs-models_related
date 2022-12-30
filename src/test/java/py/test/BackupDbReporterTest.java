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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.Before;
import org.junit.Test;
import py.RequestResponseHelper;
import py.common.RequestIdBuilder;
import py.common.Utils;
import py.common.struct.EndPoint;
import py.icshare.BackupDbReporter;
import py.icshare.BackupDbReporterImpl;
import py.icshare.DbTableName;
import py.instance.Group;
import py.thrift.share.ReportDbRequestThrift;
import py.thrift.share.ReportDbResponseThrift;

public class BackupDbReporterTest extends TestBase {
  private static final String backupDBPath = "/tmp/backupDBPath";

  @Before
  public void cleanTestEnvBefore() {
    Path pathToLogFile = FileSystems.getDefault().getPath(backupDBPath);
    try {
      Utils.deleteDirectory(pathToLogFile.toFile());
    } catch (Exception e) {
      logger.error("fail to clean env for test", e);
    }
  }

  @Test
  public void testProcessNoTable() throws Exception {
    BackupDbReporter reporter = new BackupDbReporterImpl(backupDBPath);
    assertEquals(0L, ((BackupDbReporterImpl) reporter).getSequenceId().longValue());

    // process response without any tables
    Long sequenceId = 10L;
    int domainCount = 0;
    int storagePoolCount = 0;
    int volume2RuleCount = 0;
    int accessRuleCount = 0;
    int iscsi2RuleCount = 0;
    int iscsiAccessRuleCount = 0;
    int capacityRecordCount = 0;
    int roleCount = 0;
    int accountCount = 0;
    int apiCount = 0;
    int resourceCount = 0;
    ReportDbResponseThrift response = TestUtils
        .buildReportDbResponse(sequenceId, domainCount, storagePoolCount, volume2RuleCount,
            accessRuleCount,
            capacityRecordCount, roleCount, accountCount,
            apiCount, resourceCount, iscsi2RuleCount, iscsiAccessRuleCount);
    reporter.processRsp(response);

    assertEquals(0L, ((BackupDbReporterImpl) reporter).getSequenceId().longValue());
    assertEquals(0, ((BackupDbReporterImpl) reporter).getDomainList().size());
    assertEquals(0, ((BackupDbReporterImpl) reporter).getStoragePoolList().size());
    assertEquals(0, ((BackupDbReporterImpl) reporter).getVolume2RuleList().size());
    assertEquals(0, ((BackupDbReporterImpl) reporter).getAccessRuleList().size());
    assertEquals(0, ((BackupDbReporterImpl) reporter).getIscsi2RuleList().size());
    assertEquals(0, ((BackupDbReporterImpl) reporter).getIscsiAccessRuleList().size());
    assertEquals(0, ((BackupDbReporterImpl) reporter).getCapacityRecordList().size());

    // reset memory then load file and check
    ((BackupDbReporterImpl) reporter).setSequenceId(0L);
    ((BackupDbReporterImpl) reporter).getDomainList().clear();
    ((BackupDbReporterImpl) reporter).getStoragePoolList().clear();
    ((BackupDbReporterImpl) reporter).getVolume2RuleList().clear();
    ((BackupDbReporterImpl) reporter).getAccessRuleList().clear();
    ((BackupDbReporterImpl) reporter).getIscsi2RuleList().clear();
    ((BackupDbReporterImpl) reporter).getIscsiAccessRuleList().clear();
    ((BackupDbReporterImpl) reporter).getCapacityRecordList().clear();

    // re-load database info again
    reporter.loadDbInfo();

    assertEquals(0L, ((BackupDbReporterImpl) reporter).getSequenceId().longValue());
    assertEquals(0, ((BackupDbReporterImpl) reporter).getDomainList().size());
    assertEquals(0, ((BackupDbReporterImpl) reporter).getStoragePoolList().size());
    assertEquals(0, ((BackupDbReporterImpl) reporter).getVolume2RuleList().size());
    assertEquals(0, ((BackupDbReporterImpl) reporter).getAccessRuleList().size());
    assertEquals(0, ((BackupDbReporterImpl) reporter).getIscsi2RuleList().size());
    assertEquals(0, ((BackupDbReporterImpl) reporter).getIscsiAccessRuleList().size());
    assertEquals(0, ((BackupDbReporterImpl) reporter).getCapacityRecordList().size());

  }

  @Test
  public void testProcessSomeTables() throws Exception {
    BackupDbReporter reporter = new BackupDbReporterImpl(backupDBPath);
    assertEquals(((BackupDbReporterImpl) reporter).getSequenceId().longValue(), 0L);
    // process response with some tables
    Long sequenceId = 10L;
    int domainCount = 3;
    int storagePoolCount = 0;
    int volume2RuleCount = 1;
    int accessRuleCount = 2;
    int iscsi2RuleCount = 1;
    int iscsiAccessRuleCount = 2;
    int capacityRecordCount = 1;
    int roleCount = 1;
    int accountCount = 1;
    int apiCount = 1;
    int resoureCount = 1;
    ReportDbResponseThrift response = TestUtils
        .buildReportDbResponse(sequenceId, domainCount, storagePoolCount, volume2RuleCount,
            accessRuleCount,
            capacityRecordCount, roleCount, accountCount,
            apiCount, resoureCount, iscsi2RuleCount, iscsiAccessRuleCount);
    reporter.processRsp(response);
    EndPoint endPoint = new EndPoint("10.0.1.1", 1234);
    Group group = new Group(2);
    Long instanceId = RequestIdBuilder.get();

    ReportDbRequestThrift reportRequest = reporter
        .buildReportDbRequest(endPoint, group, instanceId, true);
    assertEquals(endPoint, EndPoint.fromString(reportRequest.getEndpoint()));
    assertEquals(group, RequestResponseHelper.buildGroupFrom(reportRequest.getGroup()));
    assertEquals(instanceId.longValue(), reportRequest.getInstanceId());
    assertEquals(sequenceId.longValue(),
        ((BackupDbReporterImpl) reporter).getSequenceId().longValue());

    TestUtils.compareReportRequestAndReportResponse(reportRequest, response);
    // reset memory then load file and check
    ((BackupDbReporterImpl) reporter).setSequenceId(0L);
    ((BackupDbReporterImpl) reporter).getDomainList().clear();
    ((BackupDbReporterImpl) reporter).getStoragePoolList().clear();
    ((BackupDbReporterImpl) reporter).getVolume2RuleList().clear();
    ((BackupDbReporterImpl) reporter).getAccessRuleList().clear();

    ((BackupDbReporterImpl) reporter).getIscsi2RuleList().clear();
    ((BackupDbReporterImpl) reporter).getIscsiAccessRuleList().clear();

    ((BackupDbReporterImpl) reporter).getCapacityRecordList().clear();

    // re-load database info again
    reporter.loadDbInfo();

    reportRequest = reporter.buildReportDbRequest(endPoint, group, instanceId, true);

    // check all info
    assertEquals(endPoint, EndPoint.fromString(reportRequest.getEndpoint()));
    assertEquals(group, RequestResponseHelper.buildGroupFrom(reportRequest.getGroup()));
    assertEquals(instanceId.longValue(), reportRequest.getInstanceId());
    assertEquals(sequenceId.longValue(),
        ((BackupDbReporterImpl) reporter).getSequenceId().longValue());

    TestUtils.compareReportRequestAndReportResponse(reportRequest, response);

  }

  @Test
  public void testProcessAllTables() throws Exception {
    BackupDbReporter reporter = new BackupDbReporterImpl(backupDBPath);
    assertEquals(((BackupDbReporterImpl) reporter).getSequenceId().longValue(), 0L);

    // process response with all tables
    Long sequenceId1 = 10L;
    int domainCount = 3;
    int storagePoolCount = 3;
    int volume2RuleCount = 3;
    int accessRuleCount = 3;
    int iscsi2RuleCount = 3;
    int iscsiAccessRuleCount = 3;
    int capacityRecordCount = 1;
    int roleCount = 1;
    int accountCount = 1;
    int apiCount = 1;
    int resourceCount = 1;
    ReportDbResponseThrift response1 = TestUtils
        .buildReportDbResponse(sequenceId1, domainCount, storagePoolCount, volume2RuleCount,
            accessRuleCount,
            capacityRecordCount, roleCount, accountCount,
            apiCount, resourceCount);
    reporter.processRsp(response1);
    EndPoint endPoint = new EndPoint("10.0.1.1", 1234);
    Group group = new Group(2);
    Long instanceId = RequestIdBuilder.get();

    ReportDbRequestThrift reportRequest1 = reporter
        .buildReportDbRequest(endPoint, group, instanceId, true);
    assertEquals(endPoint, EndPoint.fromString(reportRequest1.getEndpoint()));
    assertEquals(group, RequestResponseHelper.buildGroupFrom(reportRequest1.getGroup()));
    assertEquals(instanceId.longValue(), reportRequest1.getInstanceId());
    assertEquals(sequenceId1.longValue(),
        ((BackupDbReporterImpl) reporter).getSequenceId().longValue());

    TestUtils.compareReportRequestAndReportResponse(reportRequest1, response1);
    // reset memory then load file and check
    ((BackupDbReporterImpl) reporter).setSequenceId(0L);
    ((BackupDbReporterImpl) reporter).getDomainList().clear();
    ((BackupDbReporterImpl) reporter).getStoragePoolList().clear();
    ((BackupDbReporterImpl) reporter).getVolume2RuleList().clear();
    ((BackupDbReporterImpl) reporter).getAccessRuleList().clear();
    ((BackupDbReporterImpl) reporter).getIscsi2RuleList().clear();
    ((BackupDbReporterImpl) reporter).getIscsiAccessRuleList().clear();
    ((BackupDbReporterImpl) reporter).getCapacityRecordList().clear();

    // re-load database info again
    reporter.loadDbInfo();

    reportRequest1 = reporter.buildReportDbRequest(endPoint, group, instanceId, true);

    // check all info
    assertEquals(endPoint, EndPoint.fromString(reportRequest1.getEndpoint()));
    assertEquals(group, RequestResponseHelper.buildGroupFrom(reportRequest1.getGroup()));
    assertEquals(instanceId.longValue(), reportRequest1.getInstanceId());
    assertEquals(sequenceId1.longValue(),
        ((BackupDbReporterImpl) reporter).getSequenceId().longValue());

    TestUtils.compareReportRequestAndReportResponse(reportRequest1, response1);

    ReportDbResponseThrift response2 = TestUtils
        .buildReportDbResponse(sequenceId1, domainCount, storagePoolCount, volume2RuleCount,
            accessRuleCount,
            capacityRecordCount, roleCount, accountCount,
            apiCount, resourceCount);
    reporter.processRsp(response2);
    EndPoint endPoint2 = new EndPoint("10.0.1.2", 1234);
    Group group2 = new Group(3);
    Long instanceId2 = RequestIdBuilder.get();

    ReportDbRequestThrift reportRequest2 = reporter
        .buildReportDbRequest(endPoint2, group2, instanceId2, true);
    assertEquals(endPoint2, EndPoint.fromString(reportRequest2.getEndpoint()));
    assertEquals(group2, RequestResponseHelper.buildGroupFrom(reportRequest2.getGroup()));
    assertEquals(instanceId2.longValue(), reportRequest2.getInstanceId());
    // process response with all tables again
    Long sequenceId2 = 10L;
    assertEquals(sequenceId2.longValue(),
        ((BackupDbReporterImpl) reporter).getSequenceId().longValue());

    TestUtils.compareReportRequestAndReportResponse(reportRequest2, response2);
    // reset memory then load file and check
    ((BackupDbReporterImpl) reporter).setSequenceId(0L);
    ((BackupDbReporterImpl) reporter).getDomainList().clear();
    ((BackupDbReporterImpl) reporter).getStoragePoolList().clear();
    ((BackupDbReporterImpl) reporter).getVolume2RuleList().clear();
    ((BackupDbReporterImpl) reporter).getAccessRuleList().clear();
    ((BackupDbReporterImpl) reporter).getIscsi2RuleList().clear();
    ((BackupDbReporterImpl) reporter).getIscsiAccessRuleList().clear();
    ((BackupDbReporterImpl) reporter).getCapacityRecordList().clear();

    // re-load database info again
    reporter.loadDbInfo();

    reportRequest2 = reporter.buildReportDbRequest(endPoint2, group2, instanceId2, true);

    // check all info
    assertEquals(endPoint2, EndPoint.fromString(reportRequest2.getEndpoint()));
    assertEquals(group2, RequestResponseHelper.buildGroupFrom(reportRequest2.getGroup()));
    assertEquals(instanceId2.longValue(), reportRequest2.getInstanceId());
    assertEquals(sequenceId2.longValue(),
        ((BackupDbReporterImpl) reporter).getSequenceId().longValue());

    TestUtils.compareReportRequestAndReportResponse(reportRequest2, response2);
  }

  @Test
  public void testBuildReportDbRequest() {
    BackupDbReporter reporter = new BackupDbReporterImpl(backupDBPath);
    assertEquals(((BackupDbReporterImpl) reporter).getSequenceId().longValue(), 0L);

    // build ReportDBRequest without database tables info
    EndPoint endPoint = new EndPoint("10.0.1.1", 1234);
    Group group = new Group(2);
    Long instanceId = RequestIdBuilder.get();

    ReportDbRequestThrift reportRequest = reporter
        .buildReportDbRequest(endPoint, group, instanceId, true);
    assertEquals(endPoint, EndPoint.fromString(reportRequest.getEndpoint()));
    assertEquals(group, RequestResponseHelper.buildGroupFrom(reportRequest.getGroup()));
    assertEquals(instanceId.longValue(), reportRequest.getInstanceId());
    assertEquals(0L, reportRequest.getSequenceId());
    assertTrue(!reportRequest.isSetDomainThriftList());
    assertTrue(!reportRequest.isSetStoragePoolThriftList());
    assertTrue(!reportRequest.isSetVolume2RuleThriftList());
    assertTrue(!reportRequest.isSetAccessRuleThriftList());
    assertTrue(!reportRequest.isSetIscsi2RuleThriftList());
    assertTrue(!reportRequest.isSetIscsiAccessRuleThriftList());
    assertTrue(!reportRequest.isSetCapacityRecordThriftList());

    // process response with all tables
    Long sequenceId1 = 10L;
    int domainCount = 3;
    int storagePoolCount = 3;
    int volume2RuleCount = 3;
    int accessRuleCount = 3;
    int iscsi2RuleCount = 3;
    int iscsiAccessRuleCount = 3;
    int capacityRecordCount = 1;
    int roleCount = 1;
    int accountCount = 1;
    int apiCount = 1;
    int resoureCount = 1;
    ReportDbResponseThrift response1 = TestUtils
        .buildReportDbResponse(sequenceId1, domainCount, storagePoolCount, volume2RuleCount,
            accessRuleCount,
            capacityRecordCount, roleCount, accountCount,
            apiCount, resoureCount, iscsi2RuleCount, iscsiAccessRuleCount);
    reporter.processRsp(response1);
    // build ReportDBRequest with all database tables info and check
    reportRequest = reporter.buildReportDbRequest(endPoint, group, instanceId, true);
    assertEquals(endPoint, EndPoint.fromString(reportRequest.getEndpoint()));
    assertEquals(group, RequestResponseHelper.buildGroupFrom(reportRequest.getGroup()));
    assertEquals(instanceId.longValue(), reportRequest.getInstanceId());
    assertEquals(sequenceId1.longValue(), reportRequest.getSequenceId());

    TestUtils.compareReportRequestAndReportResponse(reportRequest, response1);
  }

  @Test
  public void testSortMultiMapKeys() {
    Multimap<Long, EndPoint> firstRoundRecord = Multimaps
        .synchronizedSetMultimap(HashMultimap.<Long, EndPoint>create());
    EndPoint endPoint1 = new EndPoint("10.0.1.1", 1234);
    EndPoint endPoint2 = new EndPoint("10.0.1.2", 1234);
    EndPoint endPoint3 = new EndPoint("10.0.1.3", 1234);
    for (int i = 1; i < 100; i++) {
      firstRoundRecord.put(RequestIdBuilder.get(), endPoint1);
      firstRoundRecord.put(RequestIdBuilder.get(), endPoint2);
      firstRoundRecord.put(RequestIdBuilder.get(), endPoint3);

      firstRoundRecord.put(RequestIdBuilder.get(), endPoint1);
      firstRoundRecord.put(RequestIdBuilder.get(), endPoint3);

      firstRoundRecord.put(RequestIdBuilder.get(), endPoint2);
      firstRoundRecord.put(RequestIdBuilder.get(), endPoint3);

      firstRoundRecord.put(RequestIdBuilder.get(), endPoint1);
      firstRoundRecord.put(RequestIdBuilder.get(), endPoint2);
      firstRoundRecord.put(RequestIdBuilder.get(), endPoint3);

      List<Long> sequenceIdList = new ArrayList<Long>(firstRoundRecord.keySet());
      Collections.sort(sequenceIdList, Collections.reverseOrder());
      assertEquals(i * 10, sequenceIdList.size());

      Long compareId = sequenceIdList.get(0);
      for (Long sequenceId : sequenceIdList) {
        assertTrue(compareId >= sequenceId);
        compareId = sequenceId;
      }
    }
  }

  // It will not write to file,if the List of response is the same as memory
  @Test
  public void testProcessSave() throws Exception {
    BackupDbReporter reporter = new BackupDbReporterImpl(backupDBPath);
    assertEquals(((BackupDbReporterImpl) reporter).getSequenceId().longValue(), 0L);
    // process response with all tables
    Long sequenceId = 10L;
    int domainCount = 3;
    int storagePoolCount = 3;
    int volume2RuleCount = 2;
    int accessRuleCount = 2;
    int iscsi2RuleCount = 2;
    int iscsiAccessRuleCount = 2;
    int capacityRecordCount = 1;
    int roleCount = 1;
    int accountCount = 1;
    int apiCount = 1;
    int resoureCount = 1;
    ReportDbResponseThrift response = TestUtils
        .buildReportDbResponse(sequenceId, domainCount, storagePoolCount, volume2RuleCount,
            accessRuleCount,
            capacityRecordCount, roleCount, accountCount,
            apiCount, resoureCount, iscsi2RuleCount, iscsiAccessRuleCount);
    reporter.processRsp(response);
    EndPoint endPoint = new EndPoint("10.0.1.1", 1234);
    Group group = new Group(1);
    Long instanceId = RequestIdBuilder.get();

    ReportDbRequestThrift reportRequest = reporter
        .buildReportDbRequest(endPoint, group, instanceId, true);
    assertEquals(endPoint, EndPoint.fromString(reportRequest.getEndpoint()));
    assertEquals(group, RequestResponseHelper.buildGroupFrom(reportRequest.getGroup()));
    assertEquals(instanceId.longValue(), reportRequest.getInstanceId());
    assertEquals(sequenceId.longValue(),
        ((BackupDbReporterImpl) reporter).getSequenceId().longValue());

    // delete file

    Path stringPath = FileSystems.getDefault().getPath(backupDBPath);
    Utils.deleteEveryThingExceptDirectory(stringPath.toFile());

    reporter.processRsp(response);
    assertEquals(true,
        Files.exists(FileSystems.getDefault().getPath(backupDBPath, "sequenceId_File")));
    assertEquals(sequenceId.longValue(),
        ((BackupDbReporterImpl) reporter).getSequenceId().longValue());
    assertEquals(false,
        Files.exists(FileSystems.getDefault().getPath(backupDBPath, DbTableName.Domain.name())));
    assertEquals(false,
        Files.exists(
            FileSystems.getDefault().getPath(backupDBPath, DbTableName.StoragePool.name())));
    assertEquals(false,
        Files.exists(
            FileSystems.getDefault().getPath(backupDBPath, DbTableName.Volume2RuleRelate.name())));
    assertEquals(false,
        Files
            .exists(FileSystems.getDefault().getPath(backupDBPath, DbTableName.AccessRule.name())));
    assertEquals(false,
        Files.exists(
            FileSystems.getDefault().getPath(backupDBPath, DbTableName.CapacityRecord.name())));

    // re-load database info again
    reporter.loadDbInfo();

    assertEquals(sequenceId.longValue(),
        ((BackupDbReporterImpl) reporter).getSequenceId().longValue());
    assertEquals(0, ((BackupDbReporterImpl) reporter).getDomainList().size());
    assertEquals(0, ((BackupDbReporterImpl) reporter).getStoragePoolList().size());
    assertEquals(0, ((BackupDbReporterImpl) reporter).getVolume2RuleList().size());
    assertEquals(0, ((BackupDbReporterImpl) reporter).getAccessRuleList().size());

    assertEquals(0, ((BackupDbReporterImpl) reporter).getIscsi2RuleList().size());
    assertEquals(0, ((BackupDbReporterImpl) reporter).getIscsiAccessRuleList().size());

    assertEquals(0, ((BackupDbReporterImpl) reporter).getCapacityRecordList().size());
  }

  @Test
  public void testTwoLongGenerateUuid() {
    long offset1 = 64200704L;
    long checksum1 = 2410414467L;

    long offset2 = 64397312L;
    long checksum2 = 1470651922L;

    assertEquals(offset1 & checksum1, offset2 & checksum2);

    long tmpUuid1 = py.informationcenter.Utils.generateUuid(offset1, checksum1);
    long tmpUuid2 = py.informationcenter.Utils.generateUuid(offset1, checksum1);

    assertEquals(tmpUuid1, tmpUuid2);

    long uuid1 = py.informationcenter.Utils.generateUuid(offset1, checksum1);
    long uuid2 = py.informationcenter.Utils.generateUuid(offset2, checksum2);
    assertTrue(uuid1 != uuid2);
  }

  @Test
  public void testTwoIntGenerateUuid() {
    int highPartUuid = Math.abs((int) RequestIdBuilder.get());
    int highPartUuid1 = Math.abs((int) RequestIdBuilder.get());
    Long uuid = null;
    Long tmpUuid;
    Long uuid1;
    Set<Long> record = new HashSet<>();
    for (int i = 0; i < 10000; i++) {
      tmpUuid = py.informationcenter.Utils.generateUuid(highPartUuid, i);
      uuid1 = py.informationcenter.Utils.generateUuid(highPartUuid1, i);
      assertTrue(!record.contains(uuid1));
      assertTrue(!record.contains(tmpUuid));
      record.add(uuid1);
      record.add(tmpUuid);
      assertTrue(uuid != tmpUuid);
      uuid = tmpUuid;
      assertTrue(uuid1 != uuid);
    }
  }

}
