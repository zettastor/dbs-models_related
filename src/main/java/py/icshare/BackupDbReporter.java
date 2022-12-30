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
