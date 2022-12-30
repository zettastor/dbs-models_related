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

package py.utils;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import net.sf.json.JSONArray;
import org.junit.Test;
import py.informationcenter.Utils;
import py.monitor.common.MonitorPlatformDataDto;
import py.monitor.common.SendNoticeJobNumDto;
import py.monitor.utils.HttpClientUtils;
import py.test.TestBase;

public class UtilsTest extends TestBase {
  @Test
  public void testGenerateUuidPerformance() {
    long testSize = 100_000_000L;

    Random random = new Random(System.currentTimeMillis());
    int high = random.nextInt();
    int low = 0;

    long start = System.currentTimeMillis();
    for (long i = 0; i < testSize; i++) {
      Utils.generateUuidWithByteBuffer(high, low++);
    }
    logger.warn("count {}, cost {}", testSize, System.currentTimeMillis() - start);

    start = System.currentTimeMillis();
    for (long i = 0; i < testSize; i++) {
      Utils.generateUuid(high, low++);
    }
    logger.warn("count {}, cost {}", testSize, System.currentTimeMillis() - start);
  }

  @Test
  public void testGenerateUuid() {
    int testSize = 100;

    Random random = new Random(System.currentTimeMillis());
    int high = random.nextInt();
    int low = Integer.MAX_VALUE - random.nextInt(testSize / 2);

    // test over flow
    long uuid = Utils.generateUuid(high, low);
    for (int i = 0; i < testSize; i++) {
      long newUuid = Utils.generateUuid(high, low + 1);
      logger.info("high {}, low {}, val {}", high, low + 1, newUuid);
      if (low + 1 < low) {
        logger.info("low part overflowed {}", low);
      }
      assertEquals(1, newUuid - uuid);
      uuid = newUuid;
      low++;
    }

    // test negative to positive
    low = -random.nextInt(testSize / 2);
    uuid = Utils.generateUuid(high, low);
    for (int i = 0; i < testSize; i++) {
      long newUuid = Utils.generateUuid(high, low + 1);
      logger.info("high {}, low {}, val {}", high, low + 1, newUuid);
      if (low + 1 == 0) {
        logger.info("low part turning positive {}", low);
        long expectedDiff = -0xffffffffL; // - ((long) Integer.MAX_VALUE) * 2 - 1;
        assertEquals(expectedDiff, newUuid - uuid);
      } else {
        assertEquals(1, newUuid - uuid);
      }
      uuid = newUuid;
      low++;
    }

  }
  
  //  @Test

  public void test() throws IOException {
    final Map<String, Object> paramMap = new HashMap<>();
    SendNoticeJobNumDto sendNoticeJob = new SendNoticeJobNumDto();
    sendNoticeJob.setType("SMS");
    List<String> receiverList = new ArrayList<>();
    receiverList.add("xxx@qq.com");
    sendNoticeJob.setReceiverList(receiverList);
    sendNoticeJob.setMessage("message");
    sendNoticeJob.setAppId("123");
    Map<String, Object> extensionMap = new HashMap<>();
    extensionMap.put("subject", "xxx");
    sendNoticeJob.setExtension(extensionMap);
    sendNoticeJob.setNoticeLevel("COMMON");
    sendNoticeJob.setEmailHtmlFlag(false);
    MonitorPlatformDataDto mpd = new MonitorPlatformDataDto();
    mpd.setSendMessage(false);
    mpd.setAlarmSource("SourceName");
    mpd.setAlarmMetric("CounterKey");
    mpd.setAlarmTag("monitor_server");
    mpd.setAlarmInfo("AlarmInfo");
    mpd.setAlarmLevel("0");
    mpd.setEventType("0");
    Date date = new Date();
    String timestamp = String.valueOf(date.getTime() / 1000);
    mpd.setAlarmTime(timestamp);
    mpd.setEmail("xxx@qq.com");
    mpd.setSystem("system");
    mpd.setAlarmSysCode("AlarmSysCode");
    mpd.setAlarmName("AlarmName");
    mpd.setAlarmValue("AlarmValue");
    mpd.setAlarmThreshold("AlarmThreshold");
    sendNoticeJob.setMonitorPlatformDataDto(mpd);
    List<SendNoticeJobNumDto> sendNoticeList = new ArrayList<>();
    sendNoticeList.add(sendNoticeJob);
    paramMap.put("sendNoticeList", sendNoticeList);

    HttpClientUtils a = new HttpClientUtils(5000);
    StringBuilder response = new StringBuilder();
    HashMap<String, Object> map = new HashMap<>();
    map.put("userName", "ddd");
    Map<String, Object> headers = new HashMap<>();
    headers.put("Cookie",
        "apis=%7B%22Account%22%3A%7B%22assignRoles%22%3Atrue%2C%22assignResources%22%3Atrue%2C%22cr"
            + "eateAccount%22%3Atrue%2C%22resetAccountPassword%22%3Atrue%2C%22deleteAccounts%22%3At"
            + "rue%7D%2C%22SnapShot%22%3A%7B%22deleteSnapshot%22%3Atrue%2C%22rollbackFromSnapshot%2"
            + "2%3Atrue%2C%22createSnapshot%22%3Atrue%7D%2C%22StoragePool%22%3A%7B%22createStorageP"
            + "ool%22%3Atrue%2C%22removeArchiveFromStoragePool%22%3Atrue%2C%22updateStoragePool%22%"
            + "3Atrue%2C%22deleteStoragePool%22%3Atrue%7D%2C%22Rebalance%22%3A%7B%22getUnAppliedReb"
            + "alanceRulePool%22%3Atrue%2C%22unApplyRebalanceRule%22%3Atrue%2C%22addRebalanceRule%2"
            + "2%3Atrue%2C%22getAppliedRebalanceRulePool%22%3Atrue%2C%22applyRebalanceRule%22%3Atru"
            + "e%2C%22deleteRebalanceRule%22%3Atrue%2C%22getRebalanceRule%22%3Atrue%2C%22updateReba"
            + "lanceRule%22%3Atrue%7D%2C%22Access_Rule%22%3A%7B%22applyIscsiAccessRules%22%3Atrue%2"
            + "C%22applyVolumeAccessRuleOnVolumes%22%3Atrue%2C%22deleteVolumeAccessRules%22%3Atrue%"
            + "2C%22cancelVolumeAccessRules%22%3Atrue%2C%22deleteIscsiAccessRules%22%3Atrue%2C%22cr"
            + "eateIscsiAccessRules%22%3Atrue%2C%22cancelIscsiAccessRules%22%3Atrue%2C%22createVolu"
            + "meAccessRules%22%3Atrue%2C%22cancelVolAccessRuleAllApplied%22%3Atrue%7D%2C%22Perform"
            + "anceData%22%3A%7B%22listMultiCompressedPerformanceData%22%3Atrue%2C%22verifyReportSt"
            + "atisticsPermission%22%3Atrue%7D%2C%22License%22%3A%7B%22updateLicenseThrift%22%3Atru"
            + "e%2C%22genericLicenseSequenceNumberThrift%22%3Atrue%2C%22viewLicenseThrift%22%3Atrue"
            + "%7D%2C%22Driver%22%3A%7B%22launchDriver%22%3Atrue%2C%22umountDriver%22%3Atrue%7D%2C%"
            + "22AlertTemplate%22%3A%7B%22mergeAlertRule%22%3Atrue%2C%22updateAlertRule%22%3Atrue%2"
            + "C%22deleteAlertRule%22%3Atrue%2C%22createAlertRule%22%3Atrue%7D%2C%22AlertFoward%22%"
            + "3A%7B%22updateSnmpForwardItem%22%3Atrue%2C%22updateEmailForwardItem%22%3Atrue%2C%22s"
            + "aveOrUpdateSmtpItem%22%3Atrue%2C%22saveEmailForwardItem%22%3Atrue%2C%22deleteEmailFo"
            + "rwardItem%22%3Atrue%7D%2C%22Role%22%3A%7B%22updateRole%22%3Atrue%2C%22createRole%22%"
            + "3Atrue%2C%22deleteRoles%22%3Atrue%7D%2C%22QOS%22%3A%7B%22deleteMigrationRules%22%3At"
            + "rue%2C%22deleteIoLimitations%22%3Atrue%2C%22createMigrationRules%22%3Atrue%2C%22upda"
            + "teIoLimitations%22%3Atrue%2C%22createIoLimitations%22%3Atrue%2C%22cancelIoLimitation"
            + "s%22%3Atrue%2C%22cancelMigrationRules%22%3Atrue%2C%22applyIoLimitations%22%3Atrue%2C"
            + "%22applyMigrationRules%22%3Atrue%2C%22updateMigrationRules%22%3Atrue%7D%2C%22Volume%"
            + "22%3A%7B%22copyVolumeToExistVolume%22%3Atrue%2C%22fixVolume%22%3Atrue%2C%22extendVol"
            + "ume%22%3Atrue%2C%22createSnapshotVolume%22%3Atrue%2C%22cloneVolume%22%3Atrue%2C%22cr"
            + "eateVolume%22%3Atrue%2C%22moveVolume%22%3Atrue%2C%22confirmFixVolume%22%3Atrue%2C%22"
            + "recycleVolume%22%3Atrue%2C%22deleteVolume%22%3Atrue%7D%2C%22Hardware%22%3A%7B%22onli"
            + "neDisk%22%3Atrue%2C%22offlineDisk%22%3Atrue%2C%22markInstanceMaintenance%22%3Atrue%2"
            + "C%22cancelInstanceMaintenance%22%3Atrue%2C%22fixConfigMismatchDisk%22%3Atrue%2C%22up"
            + "dateServerNode%22%3Atrue%2C%22changeDiskLightStatus%22%3Atrue%2C%22deleteServerNodes"
            + "%22%3Atrue%7D%2C%22Domain%22%3A%7B%22removeDatanodeFromDomain%22%3Atrue%2C%22createD"
            + "omain%22%3Atrue%2C%22updateDomain%22%3Atrue%2C%22deleteDomain%22%3Atrue%7D%2C%22DTO%"
            + "22%3A%7B%22deleteDTOUsers%22%3Atrue%2C%22updateDTOUser%22%3Atrue%2C%22updateDTOUserF"
            + "lag%22%3Atrue%2C%22deleteDTOSendLog%22%3Atrue%2C%22saveDTOUser%22%3Atrue%7D%2C%22Ale"
            + "rtMessage%22%3A%7B%22alertsAcknowledge%22%3Atrue%2C%22deleteAlerts%22%3Atrue%2C%22al"
            + "ertsClear%22%3Atrue%2C%22clearAlertsAcknowledge%22%3Atrue%7D%2C%22Other%22%3A%7B%22s"
            + "aveOperationLogsToCSV%22%3Atrue%7D%7D; JSESSIONID=28BEE83D92C22EBA57F752ED14B704FE; "
            + "globals=%7B%22currentAccount%22%3A%7B%22accountId%22%3A%221862755152385798543%22%2C%"
            + "22accountName%22%3A%22admin%22%2C%22accountType%22%3A%22SuperAdmin%22%7D%7D");
    a.doPostParam(
        "http://10.0.2.117:8080/com.htsc.pt.apm.notice.api.NoticeGatewayService.sendNoticeByJobNum",
        headers, JSONArray
            .fromObject(sendNoticeList).toString(), response);
    System.out.println(response);
  }
}