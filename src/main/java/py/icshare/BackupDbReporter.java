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

package py.icshare;

import py.common.struct.EndPoint;
import py.instance.Group;
import py.thrift.share.ReportDbRequestThrift;
import py.thrift.share.ReportDbResponseThrift;

/**
 * should reduce report info data, doing this can reduce the pressure of network and server.
 *
 */
public interface BackupDbReporter {
  public ReportDbRequestThrift buildReportDbRequest(EndPoint endPoint, Group group, Long instanceId,
      boolean carryDbInfo);

  public void processRsp(ReportDbResponseThrift response);

  public void loadDbInfo() throws Exception;
}
