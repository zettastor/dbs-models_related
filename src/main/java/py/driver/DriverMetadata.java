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

package py.driver;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.common.PyService;
import py.icshare.DriverKey;
import py.informationcenter.AccessPermissionType;

/**
 * driver metadata driver such as nbd, nfs, iscsi.
 *
 * <p>driver status available means the volume has not been launched. driver status launching means
 * the volume has been launching now and the request for launching this volume will be exception.
 * driver status launch means the volume has been launched.
 */
public class DriverMetadata {
  private static final Logger logger = LoggerFactory.getLogger(DriverMetadata.class);
  /**
   * This is a memory field which records actions to be added to this driver and it should not be
   * persist to any file or directory.
   *
   * <p>Note that this class is shared among multiple project, this field is just designed for
   * service {@link PyService#DRIVERCONTAINER}.
   */
  @JsonIgnore
  private final Set<DriverAction> addingActions = new HashSet<>();
  /**
   * This is a memory field which records current action on this driver and it should not be persist
   * to any file or directory.
   *
   * <p>The purpose adding this field here is to synchronize different actions on driver instead of
   * using exclusive lock. Since actions on driver may take long time to be completed, exclusive may
   * block some other thread for a long time. And also lock will not tell which action is on
   * driver.
   *
   * <p>Note that this class is shared among multiple project, this field is just designed for
   * service {@link PyService#DRIVERCONTAINER}.
   */
  @JsonIgnore
  private final Set<DriverAction> actions = new HashSet<>();
  /**
   * add driver container for driver.
   */
  private String driverName;
  private long driverContainerId;
  private long accountId;
  private long instanceId;
  private long volumeId;
  private String volumeName;
  @JsonProperty
  private DriverKey oldDriverKey;
  private long migratingVolumeId;
  private int snapshotId;
  private DriverType driverType;
  private PortalType portalType = PortalType.IPV4;
  /**
   * Network interface card name. For IPV6, open-iscsi (initiator) needs this info to do discovery.
   * The info combined with IPV6 looks like following:
   *
   * <p>fe80::5054:ff:fe34:4f86%eth0
   *
   * <p>"fe80::5054:ff:fe34:4f86" is IPV6 address, "eth0" is network interface name. They are
   * separated by "%".
   */
  private String nicName;
  /**
   * IPV4 address binding to network interface card named {@link DriverMetadata#nicName}.
   */
  private String hostName;
  /**
   * IPV6 address binding to network interface card named {@link DriverMetadata#nicName}.
   */
  private String ipv6Addr;
  // driver port
  private int port;
  // coordinator service port
  private int coordinatorPort;
  private DriverStatus driverStatus;
  private DriverState driverState;
  private String nbdDevice;
  private int upgradePhrase;
  private String queryServerIp;
  private int queryServerPort;
  private int chapControl;

  /*
   * A map records all clients connected in.
   */
  @JsonIgnore
  private long createTime;
  @JsonIgnore
  private boolean makeUnmountForCsi;
  private Map<String, AccessPermissionType> clientHostAccessRule;
  /**
   * In linux operation system, process id 0 is out of range. Use this value as default to tell
   * others the process is not alive.
   */
  private int processId = 0;

  private long lastReportTime = 0;
  /**
   * driver qos id represent the relationship with IoLimitation one driver only hava one dynamic and
   * static IoLimitation.
   */
  private long staticIoLimitationId;

  private long dynamicIoLimitationId;

  @JsonIgnore
  private List<IscsiAccessRule> aclList = new ArrayList<>();

  /**
   * Build driver metadata from a specified file by json parser.
   *
   * @param filePath where driver metadata stored in
   * @return If something wrong occurs in the method, null value will be returned. Otherwise, a
   *          value of type {@link DriverMetadata} will be returned.
   */
  public static DriverMetadata buildFromFile(Path filePath) {
    ObjectMapper objectMapper = new ObjectMapper();
    try {
      DriverMetadata driver = objectMapper.readValue(filePath.toFile(), DriverMetadata.class);
      return driver;
    } catch (Exception e) {
      logger.error("Caught an exception when read value of driver metadata from {}",
          filePath.toString(), e);
      return null;
    }
  }

  public List<IscsiAccessRule> getAclList() {
    return aclList;
  }

  public void setAclList(List<IscsiAccessRule> aclList) {
    this.aclList = aclList;
  }

  public long getDriverContainerId() {
    return driverContainerId;
  }

  public void setDriverContainerId(long driverContainerId) {
    this.driverContainerId = driverContainerId;
  }

  public long getInstanceId() {
    return instanceId;
  }

  public void setInstanceId(long instanceId) {
    this.instanceId = instanceId;
  }

  public String getVolumeName() {
    return volumeName;
  }

  public void setVolumeName(String volumeName) {
    this.volumeName = volumeName;
  }

  public String getDriverName() {
    return driverName;
  }

  public void setDriverName(String driverName) {
    this.driverName = driverName;
  }

  public long getMigratingVolumeId() {
    return migratingVolumeId;
  }

  public void setMigratingVolumeId(long migratingVolumeId) {
    this.migratingVolumeId = migratingVolumeId;
  }

  public PortalType getPortalType() {
    return portalType;
  }

  public void setPortalType(PortalType portalType) {
    this.portalType = portalType;
  }

  public String getNicName() {
    return nicName;
  }

  public void setNicName(String nicName) {
    this.nicName = nicName;
  }

  public String getIpv6Addr() {
    return ipv6Addr;
  }

  public void setIpv6Addr(String ipv6Addr) {
    this.ipv6Addr = ipv6Addr;
  }

  /**
   * Get status of this driver.
   *
   * <p>This method is thread safe for status.
   *
   * @return status of this driver.
   */
  public synchronized DriverStatus getDriverStatus() {
    return driverStatus;
  }

  /**
   * Set status to this driver.
   *
   * <p>This method is thread safe for status.
   *
   * @return current status before set the new one
   */
  public synchronized DriverStatus setDriverStatus(DriverStatus driverStatus) {
    DriverStatus ret = this.driverStatus;

    this.driverStatus = driverStatus;
    return ret;
  }

  /**
   * Set status to this driver after checking whether the given status is the next status of current
   * base on state machine in service {@link PyService#DRIVERCONTAINER}.
   *
   * <p>This method is thread safe for status.
   *
   * @return the current status before setting the new one if the given status is the next status of
   *          current one; or {@link DriverStatus#UNKNOWN}.
   */
  public synchronized DriverStatus setDriverStatusIfValid(DriverStatus driverStatus) {
    if (driverStatus == null) {
      throw new IllegalArgumentException("Null value status!");
    }

    Set<DriverStatus> validStatuses = new HashSet<>();

    if (this.driverStatus == null) {
      validStatuses.add(DriverStatus.LAUNCHING);
    } else {
      // It is valid to set same status as the current one again.
      validStatuses.add(this.driverStatus);

      switch (this.driverStatus) {
        case LAUNCHING:
          validStatuses.add(DriverStatus.LAUNCHED);
          validStatuses.add(DriverStatus.ERROR);
          break;
        case LAUNCHED:
          validStatuses.add(DriverStatus.RECOVERING);
          validStatuses.add(DriverStatus.REMOVING);
          break;
        case ERROR:
          validStatuses.add(DriverStatus.REMOVING);
          break;
        case RECOVERING:
          validStatuses.add(DriverStatus.LAUNCHED);
          validStatuses.add(DriverStatus.REMOVING);
          break;
        case REMOVING:
          break;
        default:
      }
    }

    if (validStatuses.contains(driverStatus)) {
      DriverStatus curStatus = this.driverStatus;

      this.driverStatus = driverStatus;
      return curStatus;
    } else {
      return DriverStatus.UNKNOWN;
    }
  }

  /**
   * Set status to this driver after checking whether the given status is the next status of current
   * base on state machine in service {@link PyService#DRIVERCONTAINER}. And then record the given
   * future-action which is going to be added to the driver.
   *
   * @param driverStatus new driver status
   * @param futureAction action which is going to be added to the driver.
   * @return the current status before setting the new one if the given status is the next status of
   *          current one and no record for the given future-action before; 
   *          or {@link DriverStatus#UNKNOWN}.
   */
  public synchronized DriverStatus setDriverStatusIfValid(DriverStatus driverStatus,
      DriverAction futureAction) {
    DriverStatus curStatus = setDriverStatusIfValid(driverStatus);
    if (curStatus != DriverStatus.UNKNOWN) {
      if (this.addingActions.add(futureAction)) {
        return curStatus;
      } else {
        this.driverStatus = curStatus;
        return DriverStatus.UNKNOWN;
      }
    } else {
      return DriverStatus.UNKNOWN;
    }
  }

  /**
   * List and add all adding actions on this driver to new set. Any modification on the returning
   * set will not have effect on inner actions on driver.
   *
   * @return new set of adding actions record in driver.
   */
  public synchronized Set<DriverAction> listAndCopyAddingActions() {
    Set<DriverAction> retActions = new HashSet<>();

    retActions.addAll(addingActions);
    return retActions;
  }

  public String getHostName() {
    return hostName;
  }

  public void setHostName(String hostName) {
    this.hostName = hostName;
  }

  public int getPort() {
    return port;
  }

  public void setPort(int port) {
    this.port = port;
  }

  public DriverType getDriverType() {
    return driverType;
  }

  public void setDriverType(DriverType driverType) {
    this.driverType = driverType;
  }

  public long getVolumeId() {
    return volumeId;
  }

  public long getVolumeId(boolean oldVolPrior) {
    if (oldVolPrior && this.getOldDriverKey() != null) {
      return this.getOldDriverKey().getVolumeId();
    }
    return this.getVolumeId();
  }

  //When  oldVolPrior is true means we want get the old volumeId for this driver.
  //If oldDriverKey not null,means it has been changed volumeId for volume migration .But iscsi
  // target and pyd client for
  // ISCSI driver use the old volumeId to build targetName.

  public void setVolumeId(long volumeId) {
    this.volumeId = volumeId;
  }

  public int getSnapshotId() {
    return snapshotId;
  }

  public void setSnapshotId(int snapshotId) {
    this.snapshotId = snapshotId;
  }

  public long getLastReportTime() {
    return lastReportTime;
  }

  public void setLastReportTime(long lastReportTime) {
    this.lastReportTime = lastReportTime;
  }

  public Map<String, AccessPermissionType> getClientHostAccessRule() {
    return clientHostAccessRule;
  }

  public void setClientHostAccessRule(Map<String, AccessPermissionType> clientHostAccessRule) {
    this.clientHostAccessRule = clientHostAccessRule;
  }

  public int getProcessId() {
    return processId;
  }

  public void setProcessId(int processId) {
    this.processId = processId;
  }

  public long getAccountId() {
    return accountId;
  }

  public void setAccountId(long accountId) {
    this.accountId = accountId;
  }

  public int getCoordinatorPort() {
    return coordinatorPort;
  }

  public void setCoordinatorPort(int coordinatorPort) {
    this.coordinatorPort = coordinatorPort;
  }

  public String getNbdDevice() {
    return nbdDevice;
  }

  public void setNbdDevice(String nbdDevice) {
    this.nbdDevice = nbdDevice;
  }

  public DriverState getDriverState() {
    return driverState;
  }

  public void setDriverState(DriverState driverState) {
    this.driverState = driverState;
  }

  public int getQueryServerPort() {
    return queryServerPort;
  }

  public void setQueryServerPort(int queryServerPort) {
    this.queryServerPort = queryServerPort;
  }

  public String getQueryServerIp() {
    return queryServerIp;
  }

  public void setQueryServerIp(String queryServerIp) {
    this.queryServerIp = queryServerIp;
  }

  public int getChapControl() {
    return chapControl;
  }

  public void setChapControl(int chapControl) {
    this.chapControl = chapControl;
  }

  @JsonIgnore
  public long getCreateTime() {
    return createTime;
  }

  @JsonIgnore
  public void setCreateTime(long createTime) {
    this.createTime = createTime;
  }

  @JsonIgnore
  public boolean isMakeUnmountForCsi() {
    return makeUnmountForCsi;
  }

  @JsonIgnore
  public void setMakeUnmountForCsi(boolean makeUnmountForCsi) {
    this.makeUnmountForCsi = makeUnmountForCsi;
  }

  /**
   * List and add all existing actions on this driver to new set. Any modification on the returning
   * set will not have effect on inner actions on driver.
   *
   * <p>This method is thread safe on existing actions on this driver.
   *
   * @return new set of existing actions
   */
  public synchronized Set<DriverAction> listAndCopyActions() {
    Set<DriverAction> retActions = new HashSet<>();

    retActions.addAll(actions);
    return retActions;
  }

  /**
   * Add action to this driver.
   *
   * <p>This method is thread safe on binding actions.
   *
   * @return true if the given driver is new from all existing actions on this driver; or false if
   *          not.
   */
  public synchronized boolean addAction(DriverAction action) {
    addingActions.remove(action);
    return actions.add(action);
  }

  /**
   * Add action to this driver only if there is no conflict actions before.
   *
   * <p>This method is thread safe on binding actions.
   *
   * @return true if existing actions on this driver contains a conflict one with the given one; or
   *          false if not.
   */
  public synchronized boolean addActionWithoutConflict(DriverAction action) {
    for (DriverAction existingAction : actions) {
      if (existingAction.isConflictWith(action)) {
        return false;
      }
    }

    for (DriverAction existingAction : addingActions) {
      if (existingAction.isConflictWith(action)) {
        return false;
      }
    }

    return addAction(action);
  }

  /**
   * Remove the given action from this driver.
   *
   * <p>This method is thread safe on binding actions.
   *
   * @return true if the given driver exists on this driver; or false if not.
   */
  public synchronized boolean removeAction(DriverAction action) {
    return actions.remove(action);
  }

  public long getStaticIoLimitationId() {
    return staticIoLimitationId;
  }

  public void setStaticIoLimitationId(long staticIoLimitationId) {
    this.staticIoLimitationId = staticIoLimitationId;
  }

  public long getDynamicIoLimitationId() {
    return dynamicIoLimitationId;
  }

  public void setDynamicIoLimitationId(long dynamicIoLimitationId) {
    this.dynamicIoLimitationId = dynamicIoLimitationId;
  }

  public DriverKey getOldDriverKey() {
    return oldDriverKey;
  }

  public void setOldDriverKey(DriverKey oldDriverKey) {
    this.oldDriverKey = oldDriverKey;
  }

  /**
   * Check if existing drivers on this driver contains a conflict one with the given one.
   *
   * <p>This method is thread safe on binding actions.
   *
   * @return true if existing actions on this driver contains a conflict one with the given one; or
   *          false if not.
   */
  public synchronized boolean hasConflictActionWith(DriverAction action) {
    for (DriverAction existingAction : actions) {
      if (existingAction.isConflictWith(action)) {
        return true;
      }
    }

    return false;
  }

  public int getUpgradePhrase() {
    return upgradePhrase;
  }

  public void setUpgradePhrase(int upgradePhrase) {
    this.upgradePhrase = upgradePhrase;
  }

  /**
   * Save driver metadata to a specified file using json file.
   *
   * @param filePath where driver metadata stored in
   * @return true: if successfully save the metadata false: failed to save the metadata
   */
  public boolean saveToFile(Path filePath) {
    FileOutputStream fos = null;
    ObjectMapper objectMapper = new ObjectMapper();
    try {
      String serviceMetadataString = objectMapper.writeValueAsString(this);
      fos = new FileOutputStream(filePath.toFile());
      fos.write(serviceMetadataString.getBytes());
      fos.flush();
      fos.getFD().sync();
      return true;
    } catch (Exception e) {
      logger.error("Catch an exception when save service {} to file {}", this, filePath.toString(),
          e);
      return false;
    } finally {
      if (fos != null) {
        try {
          fos.close();
        } catch (IOException e) {
          logger.error("caught an exception when try to close FD:{}", filePath.toString(), e);
        }
      }
    }
  }

  public DriverMetadata buildNewDriver(long newVolumeId) {
    DriverMetadata driver = new DriverMetadata();
    driver.setDriverName(this.getDriverName());
    driver.setDriverContainerId(this.getDriverContainerId());
    driver.setAccountId(this.getAccountId());
    driver.setInstanceId(this.getInstanceId());
    driver.setVolumeId(newVolumeId);
    driver.setVolumeName(this.getVolumeName());
    driver.setSnapshotId(this.getSnapshotId());
    driver.setDriverType(this.getDriverType());
    driver.setHostName(this.getHostName());
    driver.setPort(this.getPort());
    driver.setCoordinatorPort(this.getCoordinatorPort());
    driver.setDriverStatus(this.getDriverStatus());
    driver.setDriverState(this.getDriverState());
    driver.setNbdDevice(this.getNbdDevice());
    driver.setUpgradePhrase(this.getUpgradePhrase());
    driver.setQueryServerIp(this.getQueryServerIp());
    driver.setQueryServerPort(this.getQueryServerPort());
    driver.setClientHostAccessRule(this.getClientHostAccessRule());
    driver.setProcessId(this.getProcessId());
    driver.setLastReportTime(this.getLastReportTime());
    driver.setStaticIoLimitationId(this.getStaticIoLimitationId());
    driver.setDynamicIoLimitationId(this.getDynamicIoLimitationId());
    driver.setChapControl(this.getChapControl());
    driver.setCreateTime(this.getCreateTime());
    driver.setMigratingVolumeId(this.getMigratingVolumeId());
    driver.setPortalType(this.getPortalType());
    driver.setNicName(this.getNicName());
    driver.setIpv6Addr(this.getIpv6Addr());
    driver.setAclList(this.getAclList());
    driver.setCreateTime(this.getCreateTime());
    driver.setMakeUnmountForCsi(this.isMakeUnmountForCsi());
    return driver;
  }

  @Override
  public String toString() {
    return "DriverMetadata{"
        + "driverName='" + driverName + '\''
        + ", driverContainerId=" + driverContainerId
        + ", accountId=" + accountId
        + ", instanceId=" + instanceId
        + ", volumeId=" + volumeId
        + ", volumeName='" + volumeName + '\''
        + ", oldDriverKey=" + oldDriverKey
        + ", migratingVolumeId=" + migratingVolumeId
        + ", snapshotId=" + snapshotId
        + ", driverType=" + driverType
        + ", portalType=" + portalType
        + ", nicName='" + nicName + '\''
        + ", hostName='" + hostName + '\''
        + ", ipv6Addr='" + ipv6Addr + '\''
        + ", port=" + port
        + ", coordinatorPort=" + coordinatorPort
        + ", driverStatus=" + driverStatus
        + ", driverState=" + driverState
        + ", nbdDevice='" + nbdDevice + '\''
        + ", upgradePhrase=" + upgradePhrase
        + ", queryServerIp='" + queryServerIp + '\''
        + ", queryServerPort=" + queryServerPort
        + ", chapControl=" + chapControl
        + ", createTime=" + createTime
        + ", forceUnmount=" + makeUnmountForCsi
        + ", clientHostAccessRule=" + clientHostAccessRule
        + ", processId=" + processId
        + ", lastReportTime=" + lastReportTime
        + ", addingActions=" + addingActions
        + ", actions=" + actions
        + ", staticIoLimitationId=" + staticIoLimitationId
        + ", dynamicIoLimitationId=" + dynamicIoLimitationId
        + ", aclList=" + aclList
        + '}';
  }

  /**
   * Build iscsi access rule list from a specified file by json parser.
   *
   * @param filePath where iscsi access rule list stored in
   * @return If something wrong occurs in the method, null value will be returned. Otherwise, a
   *          value of type {@link IscsiAccessRule } will be returned.
   */
  public List<IscsiAccessRule> buildFromFileForAclList(Path filePath) {
    ObjectMapper objectMapper = new ObjectMapper();
    try {
      aclList = objectMapper
          .readValue(filePath.toFile(), new TypeReference<List<IscsiAccessRule>>() {
          });
      for (int i = 0; i < aclList.size(); i++) {
        logger.debug("initiatorName [{}]", aclList.get(i).getInitiatorName());
      }
      logger.debug("buildFromFileForAclList [{}]", aclList);
      return aclList;
    } catch (Exception e) {
      logger.error("Caught an exception when read value of iscsi access rule list from {}",
          filePath.toString(), e);
      return null;
    }
  }

  /**
   * Save iscsi access rule list to a specified file using json file.
   *
   * @param filePath where iscsi access rule list stored in
   * @return true: if successfully save the iscsi access rule list false: failed to save the iscsi
   *          access rule list
   */
  public boolean saveToFileForAclList(Path filePath, Path fileBakPath) {
    FileOutputStream fos = null;
    ObjectMapper objectMapper = new ObjectMapper();
    try {
      if (filePath.toFile().exists()) {
        Files.copy(filePath, fileBakPath);
      }
    } catch (IOException e) {
      logger.debug("Exception happens when createFile {}", e);
    }

    try {
      String aclString = objectMapper.writeValueAsString(aclList);
      fos = new FileOutputStream(filePath.toFile());
      fos.write(aclString.getBytes());
      fos.flush();
      fos.getFD().sync();

      if (fileBakPath.toFile().exists()) {
        fileBakPath.toFile().delete();
      }
      return true;
    } catch (Exception e) {
      logger
          .error("Catch an exception when save service {} to file {}", aclList, filePath.toString(),
              e);
      return false;
    } finally {
      if (fos != null) {
        try {
          fos.close();
        } catch (IOException e) {
          logger.error("caught an exception when try to close FD:{}", filePath.toString(), e);
        }
      }
    }
  }
}
