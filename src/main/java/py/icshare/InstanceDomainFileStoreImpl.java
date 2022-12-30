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

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.app.context.InstanceDomainFileStore;
import py.instance.InstanceDomain;

public class InstanceDomainFileStoreImpl implements InstanceDomainFileStore {
  private static final Logger logger = LoggerFactory.getLogger(InstanceDomainFileStoreImpl.class);

  private InstanceDomain instanceDomain = null;
  private Path pathToFile;

  public InstanceDomainFileStoreImpl(String rootPath, String prefix) {
    pathToFile = constructFileName(rootPath, prefix);
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
        readDomainInfoFromFile();
      }

    } catch (Exception e) {
      String errMsg = "Can't get the group info from file:" + pathToFile;
      logger.error(errMsg, e);
    }
  }

  public Path getPathToFile() {
    return pathToFile;
  }

  public void setPathToFile(Path pathToFile) {
    this.pathToFile = pathToFile;
  }

  /**
   * if domainId is null, update domainId in memory, at the same time, should update file content.
   */
  @Override
  public void persistInstanceDomain(InstanceDomain instanceDomain) {
    try {
      if (instanceDomain != null) {
        if (!Files.exists(pathToFile, LinkOption.NOFOLLOW_LINKS)) {
          Files.createFile(pathToFile);
        }
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.writeValue(pathToFile.toFile(), instanceDomain);
      } else {
        // fill the config file with null
        Files.write(pathToFile, new byte[0]);
      }
      this.setInstanceDomain(instanceDomain);
      logger.info("persisted instance domain {}", instanceDomain);
    } catch (Exception e) {
      logger.error("Can't persist the this domain", e);
      String errMsg = "Can't persist the this domain id" + instanceDomain;
      throw new RuntimeException(errMsg, e);
    }
  }

  private void readDomainInfoFromFile() throws IOException {
    try {
      ObjectMapper objectMapper = new ObjectMapper();
      setInstanceDomain(
          objectMapper.readValue(new File(pathToFile.toString()), InstanceDomain.class));
    } catch (JsonParseException e1) {
      logger.info("caught json parse exception", e1);
    } catch (JsonMappingException e2) {
      logger.info("caught json map exception", e2);
    }
  }

  private Path constructFileName(String rootPath, String prefix) {
    return Paths.get(rootPath, prefix + "_domainInfo");
  }

  @Override
  public InstanceDomain getInstanceDomain() {
    return this.instanceDomain;
  }

  public void setInstanceDomain(InstanceDomain instanceDomain) {
    this.instanceDomain = instanceDomain;
  }

}
