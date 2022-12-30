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

package py.systemdaemon.common;

import java.util.List;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.test.TestBase;

/**
 * NodeInfo Tester.
 *
 * @version 1.0
 * @since <pre>Feb 25, 2019</pre>
 */
public class NodeInfoTest extends TestBase {
  private static final Logger logger = LoggerFactory.getLogger(NodeInfoTest.class);

  @Before
  public void before() throws Exception {
  }

  @After
  public void after() throws Exception {
  }

  /**
   * Method: makeNetCardInfo(String lastNetCardInfo, String iface, String ip).
   */
  @Test
  public void testMakeNetCardInfo() throws Exception {
    String iface = "eth0";
    String ip = "10.0.0.1";
    StringBuilder netCardInfo = NodeInfo.makeNetCardInfo(null, iface, ip);
    Assert.assertTrue(netCardInfo.toString().equals("eth0: 10.0.0.1"));

    iface = "eth0";
    ip = "10.0.0.1";
    netCardInfo = NodeInfo.makeNetCardInfo(new StringBuilder(), iface, ip);
    Assert.assertTrue(netCardInfo.toString().equals("eth0: 10.0.0.1"));

    iface = "eth1";
    ip = "10.0.0.2";
    netCardInfo = NodeInfo.makeNetCardInfo(netCardInfo, iface, ip);
    Assert.assertTrue(netCardInfo.toString().equals("eth0: 10.0.0.1##eth1: 10.0.0.2"));
  }

  /**
   * Method: parseNetCardInfoToGetIps(String netCardInfo).
   */
  @Test
  public void testParseNetCardInfoToGetIps() throws Exception {
    String netCardInfo = "eth0: 10.0.0.1";
    List<String> ipList = NodeInfo.parseNetCardInfoToGetIps(netCardInfo);
    Assert.assertTrue(ipList.size() == 1);
    Assert.assertTrue(ipList.get(0).equals("10.0.0.1"));

    netCardInfo = "eth0: 10.0.0.1##eth1: 10.0.0.2";
    ipList = NodeInfo.parseNetCardInfoToGetIps(netCardInfo);
    Assert.assertTrue(ipList.size() == 2);
    Assert.assertTrue(ipList.get(0).equals("10.0.0.1"));
    Assert.assertTrue(ipList.get(1).equals("10.0.0.2"));

    netCardInfo = "eth0: : 10.0.0.1##eth1: 10.0.0.2";
    ipList = NodeInfo.parseNetCardInfoToGetIps(netCardInfo);
    Assert.assertTrue(ipList.size() == 1);
    Assert.assertTrue(ipList.get(0).equals("10.0.0.2"));
  }

  /**
   * Method: makeInfoGroup(String lastInfo, String info).
   */
  @Test
  public void testMakeInfoGroup() throws Exception {
    String info = "10.0.0.1";
    StringBuilder infoGroup = NodeInfo.makeInfoGroup(null, info);
    Assert.assertTrue(infoGroup.toString().equals("10.0.0.1"));

    info = "10.0.0.1";
    infoGroup = NodeInfo.makeInfoGroup(new StringBuilder(), info);
    Assert.assertTrue(infoGroup.toString().equals("10.0.0.1"));

    info = "10.0.0.2";
    infoGroup = NodeInfo.makeInfoGroup(infoGroup, info);
    Assert.assertTrue(infoGroup.toString().equals("10.0.0.1##10.0.0.2"));
  }

  /**
   * Method: parseInfoGroup(String infoGroup).
   */
  @Test
  public void testParseInfoGroup() throws Exception {
    String infoGroup = "10.0.0.1";
    List<String> infoList = NodeInfo.parseInfoGroup(infoGroup);
    Assert.assertTrue(infoList.size() == 1);
    Assert.assertTrue(infoList.get(0).equals("10.0.0.1"));

    infoGroup = "10.0.0.1##10.0.0.2";
    infoList = NodeInfo.parseInfoGroup(infoGroup);
    Assert.assertTrue(infoList.size() == 2);
    Assert.assertTrue(infoList.get(0).equals("10.0.0.1"));
    Assert.assertTrue(infoList.get(1).equals("10.0.0.2"));

    infoGroup = "10.0.0.1##10.0.0.2#10.0.0.3";
    infoList = NodeInfo.parseInfoGroup(infoGroup);
    Assert.assertTrue(infoList.size() == 2);
    Assert.assertTrue(infoList.get(0).equals("10.0.0.1"));
    Assert.assertTrue(infoList.get(1).equals("10.0.0.2#10.0.0.3"));
  }

}
