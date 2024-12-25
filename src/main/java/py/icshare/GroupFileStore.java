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

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.app.context.GroupStore;
import py.instance.Group;

public class GroupFileStore implements GroupStore {
  private static final Logger logger = LoggerFactory.getLogger(GroupFileStore.class);

  private Group group = null;
  private Path pathToFile;

  public GroupFileStore(String root, String groupFileName) {
    pathToFile = constructFileName(root, groupFileName);
    try {
      if (!Files.exists(pathToFile, LinkOption.NOFOLLOW_LINKS)) {
        // only create this config file
        if (!Files.exists(pathToFile.getParent())) {
          Files.createDirectories(pathToFile.getParent());
        }
        Files.createFile(pathToFile);
      } else {
        // read from this config file
        // if file is empty or file content form is not suit for group.class, will throw json
        // exception
        readGroupInfoFromFile();
      }

    } catch (Exception e) {
      String errMsg = "Can't get the group info from file:" + pathToFile;
      logger.error(errMsg, e);
    }
  }

  @Override
  public Group getGroup() {
    return group;
  }

  public void setGroup(Group group) {
    this.group = group;
  }

  public Path getPathToFile() {
    return pathToFile;
  }

  public void setPathToFile(Path pathToFile) {
    this.pathToFile = pathToFile;
  }

  /**
   * if group is null, update group in memory, at the same time, should update file content.
   */
  @Override
  public void persistGroup(Group group) {
    try {
      if (group != null) {
        if (!Files.exists(pathToFile, LinkOption.NOFOLLOW_LINKS)) {
          Files.createFile(pathToFile);
        }
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.writeValue(pathToFile.toFile(), group);
      } else {
        // fill the config file with null
        Files.write(pathToFile, new byte[0]);
      }
      this.group = group;
      logger.info("persisted instance id {}", group);
    } catch (Exception e) {
      logger.error("Can't persist the this group", e);
      String errMsg = "Can't persist the this group" + group;
      throw new RuntimeException(errMsg, e);
    }
  }

  /**
   * case 1: current group is null, can update a not null new group info case 2: current group is
   * same with new group(both null), do nothing case 3: current group is same with new group(both
   * not null), do nothing case 4: current group is not same with new group, throw exception case 5：
   * current group not null, new group is null, throw exception.
   */

  @Override
  public boolean canUpdateGroup(Group newGroup) throws Exception {
    Validate.notNull(newGroup, "update group can not be null");
    // this is first deploy case
    if (this.group == null) {
      return true;
    } else {
      // current group not null
      // this is try to modify group(old group is not null)
      if (this.group.equals(newGroup)) {
        logger.info("current group:{}, try to update group:{}, do not need update", this.group,
            newGroup);
      } else {
        // can not update group
        logger.error("can not update current group:{}, with new group:{}", this.group, newGroup);
        throw new Exception();
      }
    }
    return false;
  }

  public void readGroupInfoFromFile() throws IOException {
    try {
      ObjectMapper objectMapper = new ObjectMapper();
      group = objectMapper.readValue(new File(pathToFile.toString()), Group.class);
    } catch (JsonParseException e1) {
      logger.info("caught json parse exception", e1);
    } catch (JsonMappingException e2) {
      logger.info("caught json map exception", e2);
    }
  }

  private Path constructFileName(String root, String instanceName) {
    return Paths.get(root, instanceName + "_groupInfo");
  }

  public void exitSystem() {
    System.exit(0);
  }

  @Override
  public String toString() {
    return "GroupFileStore [group=" + group + ", pathToFile=" + pathToFile + "]";
  }

}
