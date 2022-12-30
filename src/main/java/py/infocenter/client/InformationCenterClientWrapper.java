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

package py.infocenter.client;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.LongFunction;
import java.util.function.Predicate;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.RequestResponseHelper;
import py.archive.segment.CloneStatus;
import py.archive.segment.SegId;
import py.archive.segment.SegmentMetadata;
import py.archive.segment.SegmentUnitBitmap;
import py.archive.segment.SegmentUnitMetadata;
import py.common.PyService;
import py.common.RequestIdBuilder;
import py.common.struct.EndPoint;
import py.driver.DriverMetadata;
import py.exception.ServiceIsNotAvailableException;
import py.informationcenter.AccessPermissionType;
import py.instance.Group;
import py.instance.InstanceDomain;
import py.instance.InstanceId;
import py.thrift.icshare.GetDriversRequestThrift;
import py.thrift.icshare.GetDriversResponseThrift;
import py.thrift.icshare.GetVolumeRequest;
import py.thrift.icshare.GetVolumeResponse;
import py.thrift.infocenter.service.InformationCenter;
import py.thrift.infocenter.service.ReportArchivesRequest;
import py.thrift.infocenter.service.ReportArchivesResponse;
import py.thrift.infocenter.service.ReportJustCreatedSegmentUnitRequest;
import py.thrift.infocenter.service.ReportSegmentUnitCloneFailRequest;
import py.thrift.infocenter.service.ReportSegmentUnitCloneFailResponse;
import py.thrift.infocenter.service.ReportSegmentUnitRecycleFailRequest;
import py.thrift.infocenter.service.ReportSegmentUnitRecycleFailResponse;
import py.thrift.infocenter.service.ReportSegmentUnitsMetadataRequest;
import py.thrift.infocenter.service.ReportSegmentUnitsMetadataResponse;
import py.thrift.infocenter.service.ReportVolumeInfoRequest;
import py.thrift.infocenter.service.ReportVolumeInfoResponse;
import py.thrift.share.AccessDeniedExceptionThrift;
import py.thrift.share.ArchiveMetadataThrift;
import py.thrift.share.DatanodeTypeThrift;
import py.thrift.share.DriverKeyThrift;
import py.thrift.share.DriverMetadataThrift;
import py.thrift.share.GetAppliedIscsisRequest;
import py.thrift.share.GetAppliedIscsisResponse;
import py.thrift.share.GetAppliedVolumesRequest;
import py.thrift.share.GetAppliedVolumesResponse;
import py.thrift.share.GetVolumeAccessRulesRequest;
import py.thrift.share.GetVolumeAccessRulesResponse;
import py.thrift.share.InstanceMetadataThrift;
import py.thrift.share.ListVolumeAccessRulesRequest;
import py.thrift.share.ListVolumeAccessRulesResponse;
import py.thrift.share.ReportDbRequestThrift;
import py.thrift.share.SegmentUnitMetadataThrift;
import py.thrift.share.ServiceHavingBeenShutdownThrift;
import py.thrift.share.ServiceIsNotAvailableThrift;
import py.thrift.share.VolumeAccessRuleThrift;
import py.thrift.share.VolumeMetadataThrift;
import py.thrift.share.VolumeNotFoundExceptionThrift;
import py.volume.VolumeMetadata;

/**
 * A client wrapper for information center client.
 *
 */
public class InformationCenterClientWrapper {
  private static final int DEFAULT_PAGINATION_COUNT_WHEN_GET_VOLUME = 3000;
  private static Logger logger = LoggerFactory.getLogger(InformationCenterClientWrapper.class);
  private final InformationCenter.Iface delegate;

  public InformationCenterClientWrapper(InformationCenter.Iface client) {
    this.delegate = client;
  }

  public ReportArchivesResponse reportArchives(InstanceId instanceId, EndPoint endpoint,
      long capacity,
      long freeSpace, long logicalCapacity, List<ArchiveMetadataThrift> archiveMetadatas,
      Group group,
      InstanceDomain instanceDomain, ReportDbRequestThrift reportDbRequest,
      DatanodeTypeThrift dataNodeType,
      Set<Long> volumeUpdateReportTableOk)
      throws TException {
    InstanceMetadataThrift instanceMetadataThrift = new InstanceMetadataThrift(instanceId.getId(),
        endpoint.toString(), capacity, freeSpace, logicalCapacity, archiveMetadatas,
        RequestResponseHelper.buildThriftGroupFrom(group),
        RequestResponseHelper.buildThriftInstanceDomainFrom(instanceDomain), dataNodeType);

    ReportArchivesRequest request = new ReportArchivesRequest(RequestIdBuilder.get(),
        instanceMetadataThrift,
        reportDbRequest, volumeUpdateReportTableOk);
    return delegate.reportArchives(request);
  }

  public InformationCenter.Iface getClient() {
    return delegate;
  }

  public ReportVolumeInfoResponse reportVolumeInfo(ReportVolumeInfoRequest request)
      throws TException {
    return delegate.reportVolumeInfo(request);
  }

  public ReportSegmentUnitsMetadataResponse reportSegmentUnitsMetadata(InstanceId instanceId,
      Collection<SegmentUnitMetadata> segmentUnitMetadatas) throws TException {
    List<SegmentUnitMetadataThrift> segUnitMetadataThrifts = new ArrayList<>(
        segmentUnitMetadatas.size());

    for (SegmentUnitMetadata segUnitMetadata : segmentUnitMetadatas) {
      segUnitMetadataThrifts.add(RequestResponseHelper
          .buildThriftSegUnitMetadataFrom(segUnitMetadata));
    }

    ReportSegmentUnitsMetadataRequest request = new ReportSegmentUnitsMetadataRequest(
        RequestIdBuilder.get(),
        instanceId.getId(), segUnitMetadataThrifts);

    return delegate.reportSegmentUnitsMetadata(request);
  }

  public ReportSegmentUnitRecycleFailResponse reportSegmentUnitRecycleFail(
      Collection<SegmentUnitMetadata> segmentUnitMetadatas) throws TException {
    List<SegmentUnitMetadataThrift> segUnitMetadataThrifts = new ArrayList<>(
        segmentUnitMetadatas.size());
    for (SegmentUnitMetadata segUnitMetadata : segmentUnitMetadatas) {
      segUnitMetadataThrifts.add(RequestResponseHelper
          .buildThriftSegUnitMetadataFrom(segUnitMetadata));
    }
    ReportSegmentUnitRecycleFailRequest request = new ReportSegmentUnitRecycleFailRequest(
        RequestIdBuilder.get(),
        segUnitMetadataThrifts);

    return delegate.reportSegmentUnitRecycleFail(request);
  }

  public ReportSegmentUnitCloneFailResponse reportCloneFail(SegId segId, InstanceId myself)
      throws TException {
    ReportSegmentUnitCloneFailRequest request = new ReportSegmentUnitCloneFailRequest(
        RequestIdBuilder.get(),
        myself.getId(), segId.getVolumeId().getId(), segId.getIndex());
    return delegate.reportCloneFailed(request);
  }

  public VolumeMetadata getVolume(long volumeId, long accountId) {
    try {
      GetVolumeRequest getVolumeFromInfoCenterRequest = new GetVolumeRequest();
      getVolumeFromInfoCenterRequest.setRequestId(RequestIdBuilder.get());
      getVolumeFromInfoCenterRequest.setVolumeId(volumeId);
      getVolumeFromInfoCenterRequest.setAccountId(accountId);
      GetVolumeResponse getVolumeFromInfoCenterResponse = delegate
          .getVolume(getVolumeFromInfoCenterRequest);

      if (getVolumeFromInfoCenterResponse.isSetVolumeMetadata()) {
        VolumeMetadataThrift volumeMetadataThrift = getVolumeFromInfoCenterResponse
            .getVolumeMetadata();
        return RequestResponseHelper.buildVolumeFrom(volumeMetadataThrift);
      }
    } catch (Exception e) {
      logger.error("Caught an exception when get volume {}", volumeId, e);
    }
    return null;
  }

  public VolumeMetadataAndDrivers getVolumeByPagination(long volumeId, long accountId)
      throws Exception, VolumeNotFoundExceptionThrift {
    int maxTryTimeWhenStartIndexNotMove = 5;
    int sleepTimeMs = 200;
    int lastStartIndex;
    int startIndex = 0;
    VolumeMetadata volumeMetadata = null;
    VolumeMetadataAndDrivers volumeMetadataAndDrivers = new VolumeMetadataAndDrivers();

    while (true) {
      try {
        // record last start index
        lastStartIndex = startIndex;
        GetVolumeRequest getVolumeByPaginationRequest = new GetVolumeRequest();
        getVolumeByPaginationRequest.setRequestId(RequestIdBuilder.get());
        getVolumeByPaginationRequest.setVolumeId(volumeId);
        getVolumeByPaginationRequest.setAccountId(accountId);
        getVolumeByPaginationRequest.setEnablePagination(true);
        getVolumeByPaginationRequest.setStartSegmentIndex(startIndex);
        getVolumeByPaginationRequest.setPaginationNumber(DEFAULT_PAGINATION_COUNT_WHEN_GET_VOLUME);
        GetVolumeResponse getVolumeResponse = delegate.getVolume(getVolumeByPaginationRequest);
        logger.info("getVolumeByPagination, each time get the Response:{}", getVolumeResponse);

        startIndex = getVolumeResponse.getNextStartSegmentIndex();
        if (getVolumeResponse.isSetVolumeMetadata()) {
          VolumeMetadataThrift volumeMetadataThrift = getVolumeResponse.getVolumeMetadata();
          VolumeMetadata currentVolumeMetadata = RequestResponseHelper
              .buildVolumeFrom(volumeMetadataThrift);
          if (volumeMetadata == null) {
            volumeMetadata = currentVolumeMetadata;
            volumeMetadataAndDrivers.setVolumeMetadata(volumeMetadata);
          } else {
            // start index move means get more segments back
            if (startIndex > lastStartIndex) {
              for (SegmentMetadata segmentMetadata : currentVolumeMetadata.getSegments()) {
                volumeMetadata
                    .addSegmentMetadata(segmentMetadata, segmentMetadata.getLatestMembership());
              }
            }
          }
        }

        if (!getVolumeResponse.isLeftSegment()) {
          /* get the driver, use the last one **/
          List<DriverMetadata> driverMetadatas = new ArrayList<>();
          if (getVolumeResponse.getDriverMetadatas() != null
              && getVolumeResponse.getDriverMetadatas().size() > 0) {
            for (DriverMetadataThrift driverMetadataThrift : getVolumeResponse
                .getDriverMetadatas()) {
              driverMetadatas
                  .add(RequestResponseHelper.buildDriverMetadataFrom(driverMetadataThrift));
            }

            volumeMetadataAndDrivers.setDriverMetadatas(driverMetadatas);
          } //end get driver
          break;
        } else {
          // start index doesn't move this time
          if (startIndex == lastStartIndex) {
            logger.warn("request:{} to get volume:{}, but start index:{} doesn't move",
                getVolumeByPaginationRequest.getRequestId(), volumeId, startIndex);
            maxTryTimeWhenStartIndexNotMove--;
            if (maxTryTimeWhenStartIndexNotMove == 0) {
              logger.error(
                  "after several times to get volume:{}, can not get whole volume, stuck at:{}",
                  volumeId, startIndex);
              throw new Exception();
            }
            // try sleep a little time to avoid too frequently to get volume, cause infocenter too
            // many thrift client exist at the same time.
            Thread.sleep(sleepTimeMs);
          }
        }
      } catch (Exception e) {
        logger.error("caught an exception when get volume:{}", volumeId, e);
        if (e instanceof VolumeNotFoundExceptionThrift) {
          throw new VolumeNotFoundExceptionThrift();
        } else {
          throw e;
        }
      }
    }

    logger.info("getVolumeByPagination,get the end VolumeMetadataAndDrivers:{}",
        volumeMetadataAndDrivers);
    return volumeMetadataAndDrivers;
  }

  /**
   * To get volume access rules from information center.
   *
   * @param requestId id for get volume access rules request
   * @param volumeId  id for volume whose access rules are inquired
   * @return null: something wrong when get volume access rules from remote server not null: a map
   *          from remote client ip to access permission type
   */
  public Map<String, AccessPermissionType> getVolumeAccessRules(long requestId, long accountId,
      long volumeId)
      throws ServiceHavingBeenShutdownThrift {
    Map<String, AccessPermissionType> accessRuleTable = new HashMap<String, AccessPermissionType>();

    GetVolumeAccessRulesResponse response = null;
    try {
      GetVolumeAccessRulesRequest request = new GetVolumeAccessRulesRequest(requestId, accountId,
          volumeId);
      response = delegate.getVolumeAccessRules(request);
    } catch (TException e) {
      logger.error("Caught an exception when getting access rules of volume {}", volumeId, e);
      return null;
    }

    if (response.getAccessRules() != null && response.getAccessRulesSize() > 0) {
      for (VolumeAccessRuleThrift volumeAccessRuleFromRemote : response.getAccessRules()) {
        accessRuleTable.put(volumeAccessRuleFromRemote.getIncomingHostName(),
            AccessPermissionType.valueOf(volumeAccessRuleFromRemote.getPermission().name()));
      }
    }

    return accessRuleTable;
  }

  /**
   * To get volume access rule table mapping host to permission type base on specified rule id
   * list.
   *
   * <p>All element in table is rule whose id could be found in rule id list.
   *
   * <p>null: something wrong when get volume access rules from remote server
   *
   * <p>not null: a map from remote client ip to access permission type
   *
   */
  public Map<String, AccessPermissionType> getVolumeAccessRules(long requestId, long accountId,
      List<Long> ruleIdList) {
    Map<String, AccessPermissionType> accessRuleTable = new HashMap<String, AccessPermissionType>();

    ListVolumeAccessRulesRequest request = new ListVolumeAccessRulesRequest(requestId, accountId);
    ListVolumeAccessRulesResponse response = null;
    try {
      response = delegate.listVolumeAccessRules(request);
    } catch (Exception e) {
      return null;
    }
    List<VolumeAccessRuleThrift> volumeAccessRuleFromRemoteList = response.getAccessRules();
    if (volumeAccessRuleFromRemoteList != null && volumeAccessRuleFromRemoteList.size() > 0) {
      for (VolumeAccessRuleThrift accessRuleFromRemote : volumeAccessRuleFromRemoteList) {
        long idForRemoteRule = accessRuleFromRemote.getRuleId();
        if (ruleIdList.contains(idForRemoteRule)) {
          accessRuleTable.put(accessRuleFromRemote.getIncomingHostName(),
              AccessPermissionType.valueOf(accessRuleFromRemote.getPermission().name()));
        }
      }
    }

    return accessRuleTable;
  }

  /**
   * To get volume access rules from information center.
   *
   * @param requestId id for get volume access rules request
   * @param driverKey id for driver whose access rules are inquired
   * @return null: something wrong when get volume access rules from remote server ; not null: a map
   *          from remote client ip to access permission type
   */
  public Map<String, AccessPermissionType> getIscsiAccessRules(long requestId, long accountId,
      DriverKeyThrift driverKey) throws ServiceHavingBeenShutdownThrift {
    Map<String, AccessPermissionType> accessRuleTable = new HashMap<String, AccessPermissionType>();

    return accessRuleTable;
  }

  /**
   * To get volume access rule table mapping host to permission type base on specified rule id
   * list.
   *
   * <p>All element in table is rule whose id could be found in rule id list.null: something wrong
   * when get volume access rules from remote server
   *
   * <p>not null: a map from remote client ip to access permission type
   *
   */
  public Map<String, AccessPermissionType> getIscsiAccessRules(long requestId,
      List<Long> ruleIdList) {
    Map<String, AccessPermissionType> accessRuleTable = new HashMap<String, AccessPermissionType>();
    return accessRuleTable;
  }

  /**
   * Get drivers binding to the specified volume from information center.
   *
   * @param requestId id for request to get drivers
   * @param volumeId  id for specified volume
   * @return a list of driver got from information center null: something wrong
   */
  public List<DriverMetadata> getDrivers(long requestId, long volumeId)
      throws ServiceHavingBeenShutdownThrift, AccessDeniedExceptionThrift,
      VolumeNotFoundExceptionThrift {
    List<DriverMetadata> driverList = new ArrayList<>();

    GetDriversResponseThrift response;
    try {
      GetDriversRequestThrift request = new GetDriversRequestThrift(requestId, volumeId);
      response = delegate.getDrivers(request);
    } catch (AccessDeniedExceptionThrift e) {
      logger.error("Caught an exception", e);
      throw e;
    } catch (VolumeNotFoundExceptionThrift e) {
      logger.error("Caught an exception", e);
      throw e;
    } catch (ServiceHavingBeenShutdownThrift e) {
      logger.error("Caught an exception", e);
      throw e;
    } catch (TException e) {
      logger.error("Caught an exception when get drivers from informationcenter", e);
      return null;
    }

    if (response.getDrivers() != null && response.getDriversSize() > 0) {
      for (DriverMetadataThrift driverFromRemote : response.getDrivers()) {
        DriverMetadata driver = RequestResponseHelper.buildDriverMetadataFrom(driverFromRemote);
        driverList.add(driver);
      }
    }

    return driverList;
  }

  public List<Long> getAppliedVolumeIds(long requestId, long accountId, long ruleId)
      throws ServiceHavingBeenShutdownThrift {
    List<Long> volumeIdList = new ArrayList<Long>();

    GetAppliedVolumesResponse response = null;
    try {
      GetAppliedVolumesRequest request = new GetAppliedVolumesRequest(requestId, accountId, ruleId);
      response = delegate.getAppliedVolumes(request);
    } catch (TException e) {
      logger.error("Caught an exception when get applied volumes from informationcenter", e);
      return null;
    }

    if (response.getVolumeIdList() == null || response.getVolumeIdList().isEmpty()) {
      return volumeIdList;
    }

    for (long volumeId : response.getVolumeIdList()) {
      volumeIdList.add(volumeId);
    }

    return volumeIdList;
  }

  public List<DriverMetadataThrift> getAppliedIscsis(long requestId, long accountId, long ruleId)
      throws ServiceHavingBeenShutdownThrift {
    List<DriverMetadataThrift> driverList = new ArrayList<DriverMetadataThrift>();
    GetAppliedIscsisResponse response = null;
    try {
      GetAppliedIscsisRequest request = new GetAppliedIscsisRequest(requestId, accountId, ruleId);
      response = delegate.getAppliedIscsis(request);
    } catch (TException e) {
      logger.error("Caught an exception when get applied volumes from informationcenter", e);
      return null;
    }
    if (response.getDriverList() == null || response.getDriverList().isEmpty()) {
      return driverList;
    }
    for (DriverMetadataThrift driver : response.getDriverList()) {
      driverList.add(driver);
    }
    return driverList;
  }

  public void reportJustCreatedSegUnit(SegmentUnitMetadata segUnit)
      throws ServiceIsNotAvailableException,
      TException {
    ReportJustCreatedSegmentUnitRequest request = new ReportJustCreatedSegmentUnitRequest(
        RequestIdBuilder.get(),
        RequestResponseHelper.buildThriftSegUnitMetadataFrom(segUnit));

    try {
      this.delegate.reportJustCreatedSegmentUnit(request);
    } catch (ServiceHavingBeenShutdownThrift | ServiceIsNotAvailableThrift e) {
      logger.warn("Service {} is not availabe", PyService.INFOCENTER.getServiceName(), e);
      throw new ServiceIsNotAvailableException();
    }
  }

}
