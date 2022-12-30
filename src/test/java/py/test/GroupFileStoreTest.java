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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.Before;
import org.junit.Test;
import py.icshare.GroupFileStore;
import py.instance.Group;

public class GroupFileStoreTest extends TestBase {
  private GroupFileStore groupFileStoreNotExist;
  private GroupFileStore groupFileStoreExist;
  private String rootPath;
  private String fileNamePrefix;
  private Path pathToFile;

  public GroupFileStoreTest() {
  }

  @Before
  public void init() throws Exception {
    super.init();
    rootPath = "/tmp/testForGroup";
    fileNamePrefix = "datanode";
    pathToFile = Paths.get(rootPath, fileNamePrefix + "_groupInfo");
  }

  @Test
  public void testGroupFilePersistValue() throws IOException {
    // file not exists
    Files.deleteIfExists(pathToFile);
    Files.deleteIfExists(pathToFile.getParent());
    groupFileStoreNotExist = new GroupFileStore(rootPath, fileNamePrefix);
    assertEquals(null, groupFileStoreNotExist.getGroup());
    // file exists
    groupFileStoreExist = new GroupFileStore(rootPath, fileNamePrefix);
    assertEquals(null, groupFileStoreExist.getGroup());
    // file exists and write group(json form) data to file
    int groupId = 5;
    Group group = new Group(groupId);
    groupFileStoreExist.persistGroup(group);
    groupFileStoreExist.readGroupInfoFromFile();
    assertEquals(groupId, groupFileStoreExist.getGroup().getGroupId());
  }

  @Test
  public void testGroupFilePersistNull() throws IOException {
    // file not exists
    Files.deleteIfExists(pathToFile);
    Files.deleteIfExists(pathToFile.getParent());
    // file exists
    groupFileStoreExist = new GroupFileStore(rootPath, fileNamePrefix);
    assertEquals(null, groupFileStoreExist.getGroup());
    // file exists and write group(json form) data to file
    Group group = new Group(19);
    groupFileStoreExist.persistGroup(group);
    groupFileStoreExist.readGroupInfoFromFile();
    assertEquals(19, groupFileStoreExist.getGroup().getGroupId());
    group = null;
    groupFileStoreExist.persistGroup(group);
    assertEquals(null, groupFileStoreExist.getGroup());
    // clean UT env
    Files.deleteIfExists(pathToFile);
    Files.deleteIfExists(pathToFile.getParent());
  }

  public static class GroupFileStoreTestForExit extends GroupFileStore {
    private boolean exitFlag;

    public GroupFileStoreTestForExit(String root, String groupFileName) {
      super(root, groupFileName);
    }

    @Override
    public void exitSystem() {
      exitFlag = true;
    }

    public boolean isExitFlag() {
      return exitFlag;
    }

    public void setExitFlag(boolean exitFlag) {
      this.exitFlag = exitFlag;
    }
  }

}
