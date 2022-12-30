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

import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import io.netty.buffer.ByteBuf;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.netty.datanode.PyReadResponse;
import py.proto.Broadcastlog.PbBroadcastLog;
import py.proto.Broadcastlog.PbIoUnitResult;
import py.proto.Broadcastlog.PbReadResponse;
import py.proto.Broadcastlog.PbReadResponseUnit;

@Deprecated
public class ReadMergeHelper {
  private static final Logger logger = LoggerFactory.getLogger(ReadMergeHelper.class);

  protected static PlansToApplyLogsForMerge generatePlans(int pageSize,
      TreeSet<PbBroadcastLog> logs) {
    PlansToApplyLogsForMerge plans = new PlansToApplyLogsForMerge();
    //the set save the ranges that are alreay occupied by prev logs who has bigger log id
    RangeSet<Integer> rangesCovered = TreeRangeSet.create();

    // iterate all logs from the log which has the largest log id
    Iterator<PbBroadcastLog> iter = logs.descendingIterator();
    while (iter.hasNext()) {
      PbBroadcastLog log = iter.next();

      logger.debug("generate plan of the log {}", log);

      int logOffsetAtPage = (int) (log.getOffset() % pageSize);

      // The range that log data want covers
      Range<Integer> dataRangeForPageData = Range
          .closedOpen(logOffsetAtPage, logOffsetAtPage + log.getLength());
      // but only the complement range are left for you
      RangeSet<Integer> complementRangeSet = rangesCovered.complement();
      // the range that the log data alowed to cover is subRangeSet()
      RangeSet<Integer> rangeSetForPageData = TreeRangeSet.create();
      rangeSetForPageData.addAll(complementRangeSet.subRangeSet(dataRangeForPageData));
      logger.debug("range set {}", rangeSetForPageData);

      plans.addPlan(log, rangeSetForPageData);
      rangesCovered.add(dataRangeForPageData);
    }

    logger.debug("generatePlans plan is {}", plans.getPlans());
    return plans;
  }

  /**
   * merge the two response from secondary after the primary is down or checksum mismatch, in pss
   * only this method is new write from this commit 2018.5.29
   *
   * @param responses responses from two s
   */
  public static PyReadResponse merge(PyReadResponse[] responses, int pageSize) {
    if (responses.length <= 1) {
      logger.debug("there is no need merging {}", responses[0].getMetadata());
      return responses[0];
    }

    if (responses.length != 2) {
      for (PyReadResponse pyReadResponse : responses) {
        if (pyReadResponse != null) {
          pyReadResponse.release();
        }
      }
      throw new IllegalArgumentException(
          "why there are more than 2 response to merge?" + responses.length);
    }
    // select a response as a base response which has max log id.
    // because, logsToMerge is from pcl, so if you take the one who has smaller pcl, maybe you
    // would miss a log whose id is just between smaller pcl and larger pcl, and unfortunately the
    // log only exist in the other S
    PyReadResponse baseResponse = responses[0];
    PyReadResponse otherResponse = responses[1];
    PbReadResponse.Builder responseBuilder = baseResponse.getMetadata().toBuilder();
    Map<Integer, ByteBuf> mapIndexToBuffer = new HashMap<>(baseResponse.getResponseUnitsCount());
    Map<Integer, DataType> mapIndexToDataType = new HashMap<>(baseResponse.getResponseUnitsCount());
    try {
      Validate
          .isTrue(baseResponse.getResponseUnitsCount() == otherResponse.getResponseUnitsCount());
      if (baseResponse.getMetadata().getPclId() < otherResponse.getMetadata().getPclId()) {
        PyReadResponse tmp = otherResponse;
        otherResponse = baseResponse;
        baseResponse = tmp;
      }

      logger.info("need merging one {}, other {}", baseResponse.getMetadata(),
          otherResponse.getMetadata());
      // use a map to put the merge data
      // use the base builder who has metadata while not build a new one(or it will miss metadata)
      responseBuilder.clearResponseUnits();
      TreeSet<PbBroadcastLog> logsMerged = new TreeSet<>(
          Comparator.comparingLong(PbBroadcastLog::getLogId));
      for (int i = 0; i < baseResponse.getResponseUnitsCount(); i++) {
        PbReadResponseUnit baseReadUnit = baseResponse.getMetadata().getResponseUnits(i);
        PbReadResponseUnit.Builder unitBuilder = baseReadUnit.toBuilder();
        PbReadResponseUnit otherReadUnit = otherResponse.getMetadata().getResponseUnits(i);
        /*
         * the two results have five combinations
         * 1. OK + OK => Merge
         * 2. OK + Free => OK
         * 3. Free + Ok => OK
         * 4. Free + Free => Free
         * 5. not the above => MergeFail. no matter there is ok in them or not, it must be fail
         */
        if (baseReadUnit.getResult() == PbIoUnitResult.OK
            && otherReadUnit.getResult() == PbIoUnitResult.OK) {
          logger.info("both the base {}, and the other {} are ok, need merge", baseReadUnit,
              otherReadUnit);
          mapIndexToDataType.put(i, DataType.Merged);
        } else if (baseReadUnit.getResult() == PbIoUnitResult.OK
            && otherReadUnit.getResult() == PbIoUnitResult.FREE) {
          logger.info("the base {} is ok, but the other {} is free", baseReadUnit, otherReadUnit);
          responseBuilder.addResponseUnits(baseReadUnit);
          mapIndexToDataType.put(i, DataType.FromBase);
          continue;
        } else if (baseReadUnit.getResult() == PbIoUnitResult.FREE
            && otherReadUnit.getResult() == PbIoUnitResult.OK) {
          logger.info("the base {} is free, but the other {} is ok", baseReadUnit, otherReadUnit);
          responseBuilder.addResponseUnits(otherReadUnit);
          mapIndexToDataType.put(i, DataType.FromOther);
          continue;
        } else if (baseReadUnit.getResult() == PbIoUnitResult.FREE
            && otherReadUnit.getResult() == PbIoUnitResult.FREE) {
          logger.info("both the base {}, and the other {} are free", baseReadUnit, otherReadUnit);
          responseBuilder.addResponseUnits(unitBuilder.setResult(PbIoUnitResult.FREE).build());
          continue;
        } else {
          logger.warn("there is someone in base {} and other {} is not ok", baseReadUnit,
              otherReadUnit);
          responseBuilder
              .addResponseUnits(unitBuilder.setResult(PbIoUnitResult.MERGE_FAIL).build());
          continue;
        }

        // first save the base logs to a set
        // not use hash set for Pb data structures
        Set<PbBroadcastLog> logsInBaseUnitSet = new TreeSet<>(
            Comparator.comparingLong(PbBroadcastLog::getLogId));
        logsInBaseUnitSet.addAll(baseReadUnit.getLogsToMergeList());

        //second add both base and other to merge set
        logsMerged.clear();
        logsMerged.addAll(logsInBaseUnitSet);
        logsMerged.addAll(otherReadUnit.getLogsToMergeList());
        logger.debug("logsToMerge:{}  base {} other {}", logsMerged, logsInBaseUnitSet,
            otherReadUnit.getLogsToMergeList());
        ByteBuf baseBuffer = baseResponse.getResponseUnitDataWithoutRetain(i);
        ByteBuf otherBuffer = otherResponse.getResponseUnitDataWithoutRetain(i);
        try {
          PlansToApplyLogsForMerge plans = generatePlans(pageSize, logsMerged);

          for (PlansToApplyLogsForMerge.Plan plan : plans.getPlans()) {
            //the base log data already in base buffer
            if (logsInBaseUnitSet.contains(plan.log)) {
              logger.debug("this log belongs to base unit, no need apply {}", plan.log);
            } else {
              //the other log data is in other buffer, should copy to base buffer
              applyPlan(plan, otherBuffer, baseBuffer);
            }
          }
          mapIndexToBuffer.put(i, baseBuffer);
          responseBuilder.addResponseUnits(unitBuilder.setResult(PbIoUnitResult.MERGE_OK).build());
        } catch (Exception e) {
          logger.error("", e);
          responseBuilder
              .addResponseUnits(unitBuilder.setResult(PbIoUnitResult.MERGE_FAIL).build());
        }
      }
      return new PyReadResponseMerger(responseBuilder.build(), baseResponse, otherResponse,
          mapIndexToBuffer, mapIndexToDataType);
    } catch (Throwable t) {
      logger.error("caught an exception when merge read responses", t);
      baseResponse.release();
      otherResponse.release();
      return null;
    }

  }

  /**
   * just copy from page.write() refers to MemoryPageImpl
   *
   * @param plan   contains a log, and its range set to apply data
   * @param srcBuf the log data
   * @param desBuf copy log data to other buffer
   */
  protected static void applyPlan(PlansToApplyLogsForMerge.Plan plan, ByteBuf srcBuf,
      ByteBuf desBuf) {
    logger.debug("deal with other's log {}", plan.getLog());
    for (Range<Integer> range : plan.ranges.asRanges()) {
      logger.debug("deal with range {}", range);
      int lowerEndPoint =
          range.lowerBoundType() == BoundType.CLOSED ? range.lowerEndpoint()
              : range.lowerEndpoint() + 1;
      int upperEndPoint =
          range.upperBoundType() == BoundType.CLOSED ? range.upperEndpoint()
              : range.upperEndpoint() - 1;

      srcBuf.getBytes(lowerEndPoint, desBuf, lowerEndPoint, upperEndPoint - lowerEndPoint + 1);
    }
  }

  /**
   * to distinguish the data location in PYReadResponseMerger.
   */
  enum DataType {
    FromBase,
    FromOther,
    Merged
  }

  protected static class PlansToApplyLogsForMerge {
    private List<Plan> plans;

    PlansToApplyLogsForMerge() {
      plans = new ArrayList<>();
    }

    void addPlan(PbBroadcastLog log, RangeSet<Integer> ranges) {
      plans.add(new Plan(log, ranges));
    }

    public List<Plan> getPlans() {
      return this.plans;
    }

    public class Plan {
      final PbBroadcastLog log;
      final RangeSet<Integer> ranges;

      Plan(PbBroadcastLog log, RangeSet<Integer> ranges) {
        this.log = log;
        this.ranges = ranges;
      }

      public PbBroadcastLog getLog() {
        return this.log;
      }

      public RangeSet<Integer> getRangesToApply() {
        return this.ranges;
      }

      public String toString() {
        return "Plan [log=" + log + ", ranges=" + ranges + "]";
      }
    }
  }

  /**
   * wrapper two PYReadResponses the metadata is new build from baseResponse, the data of index
   * maybe in base, other, or the merged buffer saved in mapIndexToBuffer.
   */
  static class PyReadResponseMerger extends PyReadResponse {
    private final Map<Integer, DataType> mapIndexToDataType;
    private final PyReadResponse otherResponsse;
    private final PyReadResponse baseResponsse;
    private final Map<Integer, ByteBuf> mapIndexToMergedBuffer;

    public PyReadResponseMerger(PbReadResponse metadata, PyReadResponse baseResponsse,
        PyReadResponse otherResponsse,
        Map<Integer, ByteBuf> mapIndexToBuffer, Map<Integer, DataType> mapIndexToDataType) {
      super(metadata, null, true);
      this.mapIndexToMergedBuffer = mapIndexToBuffer;
      this.mapIndexToDataType = mapIndexToDataType;
      this.otherResponsse = otherResponsse;
      this.baseResponsse = baseResponsse;
    }

    @Override
    public ByteBuf getResponseUnitData(int index) {
      DataType dataType = mapIndexToDataType.get(index);
      if (dataType == null) {
        logger.error("can not read from base {} other {}, merged {}",
            baseResponsse.getMetadata().getResponseUnits(index).getResult(),
            otherResponsse.getMetadata().getResponseUnits(index).getResult(),
            getMetadata().getResponseUnits(index).getResult());
        return null;
      }

      switch (dataType) {
        case FromBase:
          return baseResponsse.getResponseUnitData(index);
        case FromOther:
          return otherResponsse.getResponseUnitData(index);
        case Merged:
          logger.info("get merged buffer for index={}", index);
          return mapIndexToMergedBuffer.get(index).retain();
        default:
          return null;
      }
    }

    public void release() {
      baseResponsse.release();
      otherResponsse.release();
    }
  }

}
