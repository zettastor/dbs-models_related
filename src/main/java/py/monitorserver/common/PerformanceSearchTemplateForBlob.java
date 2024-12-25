/**
* Copyright (C) 2013-2024 Nanjing Pengyun Network Technology Co., Ltd.
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/ 

package py.monitorserver.common;

import java.nio.charset.StandardCharsets;
import java.sql.Blob;
import java.sql.SQLException;
import java.util.Date;
import org.hibernate.LobHelper;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.monitor.common.PerformanceSearchTemplate;

public class PerformanceSearchTemplateForBlob {
  private static final Logger logger = LoggerFactory
      .getLogger(PerformanceSearchTemplateForBlob.class);

  private long id;
  private String name;
  private Date startTime;
  private Date endTime;
  private long period;
  private String timeUnit;
  private String counterKeyJson;
  private Blob sourcesJson;
  private long accountId;
  private String objectType;

  public static PerformanceSearchTemplate fromBlob(
      PerformanceSearchTemplateForBlob templateForBlob) {
    PerformanceSearchTemplate performanceSearchTemplate = new PerformanceSearchTemplate();
    performanceSearchTemplate.setId(templateForBlob.getId());
    performanceSearchTemplate.setName(templateForBlob.getName());
    performanceSearchTemplate.setStartTime(templateForBlob.getStartTime());
    performanceSearchTemplate.setEndTime(templateForBlob.getEndTime());
    performanceSearchTemplate.setPeriod(templateForBlob.getPeriod());
    performanceSearchTemplate.setTimeUnit(templateForBlob.getTimeUnit());
    performanceSearchTemplate.setCounterKeyJson(templateForBlob.getCounterKeyJson());
    try {
      performanceSearchTemplate.setSourcesJson(new String(templateForBlob.getSourcesJson()
          .getBytes(1, (int) templateForBlob.getSourcesJson().length())));
    } catch (SQLException e) {
      logger.error("getPerformanceSearchTemplateByName failed, SourcesJson blob to string error");
    }
    performanceSearchTemplate.setAccountId(templateForBlob.getAccountId());
    performanceSearchTemplate.setObjectType(templateForBlob.getObjectType());
    return performanceSearchTemplate;
  }

  public static PerformanceSearchTemplateForBlob toBlob(PerformanceSearchTemplate template,
      SessionFactory sessionFactory) {
    PerformanceSearchTemplateForBlob templateForBlob = new PerformanceSearchTemplateForBlob();
    templateForBlob.setId(template.getId());
    templateForBlob.setName(template.getName());
    templateForBlob.setStartTime(template.getStartTime());
    templateForBlob.setEndTime(template.getEndTime());
    templateForBlob.setPeriod(template.getPeriod());
    templateForBlob.setTimeUnit(template.getTimeUnit());
    templateForBlob.setCounterKeyJson(template.getCounterKeyJson());
    Session session = sessionFactory.openSession();
    LobHelper lobHelper = session.getLobHelper();
    Blob sourcesJson = lobHelper.createBlob(template.getSourcesJson().getBytes(
        StandardCharsets.UTF_8));
    templateForBlob.setSourcesJson(sourcesJson);
    templateForBlob.setAccountId(template.getAccountId());
    templateForBlob.setObjectType(template.getObjectType());
    return templateForBlob;
  }

  public long getId() {
    return id;
  }

  public void setId(long id) {
    this.id = id;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public Date getStartTime() {
    return startTime;
  }

  public void setStartTime(Date startTime) {
    this.startTime = startTime;
  }

  public Date getEndTime() {
    return endTime;
  }

  public void setEndTime(Date endTime) {
    this.endTime = endTime;
  }

  public long getPeriod() {
    return period;
  }

  public void setPeriod(long period) {
    this.period = period;
  }

  public String getTimeUnit() {
    return timeUnit;
  }

  public void setTimeUnit(String timeUnit) {
    this.timeUnit = timeUnit;
  }

  public String getCounterKeyJson() {
    return counterKeyJson;
  }

  public void setCounterKeyJson(String counterKeyJson) {
    this.counterKeyJson = counterKeyJson;
  }

  public Blob getSourcesJson() {
    return sourcesJson;
  }

  public void setSourcesJson(Blob sourcesJson) {
    this.sourcesJson = sourcesJson;
  }

  public long getAccountId() {
    return accountId;
  }

  public void setAccountId(long accountId) {
    this.accountId = accountId;
  }

  public String getObjectType() {
    return objectType;
  }

  public void setObjectType(String objectType) {
    this.objectType = objectType;
  }
}
