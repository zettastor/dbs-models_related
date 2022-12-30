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

package py.datanode.client;

import com.google.common.base.Optional;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.lang3.Validate;
import org.apache.thrift.async.AsyncMethodCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.RequestResponseHelper;
import py.archive.segment.SegmentLeaseHandler;
import py.client.thrift.GenericThriftClientFactory;
import py.common.Utils;
import py.common.struct.EndPoint;
import py.exception.FailedToSendBroadcastRequestsException;
import py.exception.GenericThriftClientFactoryException;
import py.exception.QuorumNotFoundException;
import py.exception.SnapshotVersionMissMatchForMergeLogsException;
import py.instance.InstanceId;
import py.membership.SegmentMembership;
import py.thrift.datanode.service.BroadcastRequest;
import py.thrift.datanode.service.BroadcastResponse;
import py.thrift.datanode.service.BroadcastTypeThrift;
import py.thrift.datanode.service.DataNodeService;
import py.thrift.datanode.service.DataNodeService.AsyncClient.broadcast_call;
import py.thrift.datanode.service.InvalidSegmentUnitStatusExceptionThrift;
import py.thrift.datanode.service.LeaseExtensionFrozenExceptionThrift;
import py.thrift.datanode.service.SegmentNotFoundExceptionThrift;
import py.thrift.datanode.service.WriteMutationLogExceptionThrift;
import py.thrift.datanode.service.WriteMutationLogFailReasonThrift;
import py.thrift.share.LogIdTooSmallExceptionThrift;
import py.thrift.share.PrimaryExistsExceptionThrift;
import py.thrift.share.ResourceExhaustedExceptionThrift;
import py.thrift.share.SegmentMembershipThrift;

/**
 * Implement datanode's async interface, but supports sending requests to multiple clients and
 * determining whether the quorum of end points have returned response.
 *
 * <p>Note: this class only supports broadcast. It should be easy to extend it to support other
 * methods.
 *
 */
public class DataNodeServiceAsyncClientWrapper {
  protected static final int MIN_BACKOFF_UNIT_MS = 100; // ms
  private static final Logger logger = LoggerFactory
      .getLogger(DataNodeServiceAsyncClientWrapper.class);
  private static final int EXCEPTION_RECORD_PER_NUM = 200;
  private final GenericThriftClientFactory<DataNodeService.AsyncIface> clientFactory;
  private int exceptionCounter = 0;

  public DataNodeServiceAsyncClientWrapper(
      GenericThriftClientFactory<DataNodeService.AsyncIface> clientFactory) {
    this.clientFactory = clientFactory;
  }

  public static boolean checkResultAccordingToMembership(Collection<EndPoint> goodEndPoints,
      Map<EndPoint, InstanceId> mapEndPointToInstanceId, SegmentMembership membership,
      int quorumSize) {
    int numSecondaries = 0;
    int numJoiningSecondaries = 0;
    int numArbiters = 0;
    for (EndPoint ep : goodEndPoints) {
      InstanceId instanceId = mapEndPointToInstanceId.get(ep);
      if (membership.isSecondary(instanceId)) {
        numSecondaries++;
      } else if (membership.isJoiningSecondary(instanceId)) {
        numJoiningSecondaries++;
      } else if (membership.isArbiter(instanceId)) {
        numArbiters++;
      } else {
        throw new IllegalArgumentException(instanceId + " @@ " + membership.toString());
      }
    }
    return membership
        .checkWriteResultOfSecondariesAndArbiters(quorumSize, numSecondaries, numJoiningSecondaries,
            numArbiters);
  }

  public static boolean tooManyBadAccordinigToMembership(Collection<EndPoint> badEndPoints,
      Map<EndPoint, InstanceId> mapEndPointToInstanceId, SegmentMembership membership,
      int quorumSize) {
    int numBadSecondaries = 0;
    int numBadJoiningSecondaries = 0;
    int numBadArbiters = 0;
    for (EndPoint ep : badEndPoints) {
      InstanceId instanceId = mapEndPointToInstanceId.get(ep);
      if (membership.isSecondary(instanceId)) {
        numBadSecondaries++;
      } else if (membership.isArbiter(instanceId)) {
        numBadArbiters++;
      } else if (membership.isJoiningSecondary(instanceId)) {
        numBadJoiningSecondaries++;
      }
    }
    logger.debug("not enough members ? {} {} {}", quorumSize, numBadSecondaries,
        numBadJoiningSecondaries);
    return membership.checkBadWriteResultOfSecondariesAndArbiters(quorumSize, numBadSecondaries,
        numBadJoiningSecondaries, numBadArbiters)
        || badEndPoints.size() == mapEndPointToInstanceId.size();
  }

  public BroadcastResult broadcast(BroadcastRequest request, long timeout, int quorumSize,
      boolean exitEarlier,
      EndPoint... eps)
      throws QuorumNotFoundException, FailedToSendBroadcastRequestsException,
      SnapshotVersionMissMatchForMergeLogsException {
    return broadcast(Optional.absent(), 0, request, timeout, quorumSize, exitEarlier,
        Arrays.asList(eps),
        Optional.absent(), Optional.absent());
  }

  public BroadcastResult broadcast(BroadcastRequest request, long timeout, int quorumSize,
      EndPoint... eps)
      throws QuorumNotFoundException, FailedToSendBroadcastRequestsException,
      SnapshotVersionMissMatchForMergeLogsException {
    return broadcast(Optional.absent(), 0, request, timeout, quorumSize, true, Arrays.asList(eps),
        Optional.absent(), Optional.absent());
  }

  public BroadcastResult broadcast(BroadcastRequest request, long timeout, int quorumSize,
      boolean exitEarlier,
      Collection<EndPoint> eps)
      throws QuorumNotFoundException, FailedToSendBroadcastRequestsException,
      SnapshotVersionMissMatchForMergeLogsException {
    return broadcast(Optional.absent(), 0, request, timeout, quorumSize, exitEarlier, eps,
        Optional.absent(),
        Optional.absent());
  }

  public BroadcastResult broadcast(SegmentLeaseHandler segmentLeaseHandler, int leaseSpanForPeer,
      BroadcastRequest request, long timeout, int quorumSize, boolean exitEarlier,
      List<EndPoint> eps)
      throws QuorumNotFoundException, FailedToSendBroadcastRequestsException,
      SnapshotVersionMissMatchForMergeLogsException {
    return broadcast(Optional.of(segmentLeaseHandler), leaseSpanForPeer, request, timeout,
        quorumSize, exitEarlier,
        eps, Optional.absent(), Optional.absent());
  }

  public BroadcastResult broadcast(BroadcastRequest request, long timeout, int quorumSize,
      boolean exitEarlier,
      Collection<EndPoint> eps, SegmentMembership membership,
      Map<EndPoint, InstanceId> mapEndPointToInstanceId)
      throws QuorumNotFoundException, FailedToSendBroadcastRequestsException,
      SnapshotVersionMissMatchForMergeLogsException {
    return broadcast(Optional.absent(), 0, request, timeout, quorumSize, exitEarlier, eps,
        Optional.of(membership),
        Optional.of(mapEndPointToInstanceId));
  }

  public BroadcastResult broadcast(Optional<SegmentLeaseHandler> segmentLeaseHandler,
      int leaseSpanForPeer,
      BroadcastRequest request, long requestTimeout, int quorumSize, boolean exitEarlier,
      Collection<EndPoint> eps, Optional<SegmentMembership> membership,
      Optional<Map<EndPoint, InstanceId>> mapEndPointToInstanceId)
      throws QuorumNotFoundException, FailedToSendBroadcastRequestsException,
      SnapshotVersionMissMatchForMergeLogsException {
    boolean notEnoughMembers = false;
    if (membership.isPresent() && mapEndPointToInstanceId.isPresent()) {
      notEnoughMembers = !checkResultAccordingToMembership(mapEndPointToInstanceId.get().keySet(),
          mapEndPointToInstanceId.get(), membership.get(), quorumSize);
    } else {
      notEnoughMembers = (eps == null || quorumSize > eps.size());
    }

    if (notEnoughMembers) {
      logger
          .warn("can't broadcast successfully because there are not enough members end points : {}",
              eps);
      BroadcastResult result = new BroadcastResult();
      result.broadcastSucceeded = false;
      throw new QuorumNotFoundException(result);
    }

    ResponseCollector<EndPoint, BroadcastResponse> responseCollector = new ResponseCollector<>();

    int numRequestSent = 0;
    CountDownLatch latch = new CountDownLatch(eps.size());

    for (EndPoint endPointToBroadcast : eps) {
      try {
        // the timeout that is passed to the client might be larger than what the caller expected,
        // since this function might have slept for a while because of backoff. I think it is ok to
        // do so
        DataNodeService.AsyncIface asyncClient = getClient(endPointToBroadcast, requestTimeout);
        AbstractBroadcastCallback callback;
        if (membership.isPresent() && mapEndPointToInstanceId.isPresent()) {
          callback = new BroadcastMethodCallbackWithMembership(responseCollector, request,
              endPointToBroadcast, eps.size(), latch, exitEarlier, quorumSize, membership.get(),
              mapEndPointToInstanceId.get());
        } else {
          callback = new BroadcastMethodCallback(segmentLeaseHandler, leaseSpanForPeer, request,
              endPointToBroadcast, responseCollector, eps.size(), quorumSize, exitEarlier, latch);
        }
        asyncClient.broadcast(request, callback);
        logger.debug("sent endpoint: {} a request {} , socket timeout: {}", endPointToBroadcast,
            request,
            requestTimeout);
        numRequestSent++;
      } catch (Exception e) {
        responseCollector.addClientSideThrowable(endPointToBroadcast, e);
        if (++exceptionCounter % EXCEPTION_RECORD_PER_NUM == 0) {
          logger.warn(
              "Caught an exception while sending a broadcast request {} to the end point {} this is"
                  + " {} such exceptions {}, exception occur times {}",
              request, endPointToBroadcast, responseCollector.numBadResponses(),
              e.getClass().getSimpleName(), exceptionCounter);
        }
      }
    }

    boolean tooManyBad = false;
    if (membership.isPresent() && mapEndPointToInstanceId.isPresent()) {
      tooManyBad = tooManyBadAccordinigToMembership(responseCollector.getBadOnes(),
          mapEndPointToInstanceId.get(),
          membership.get(), quorumSize);
    } else {
      tooManyBad = numRequestSent < quorumSize;
    }

    if (tooManyBad) {
      logger.warn(
          "Wanted to broadcast to {}, but it's impossible because there are already too many "
              + "requests failed {}",
          eps.size(), eps.size() - numRequestSent);
      BroadcastResult result = null;
      boolean hasSnpashotVersionException = false;
      if (request.getLogType() == BroadcastTypeThrift.WriteMutationLogs
          || request.getLogType() == BroadcastTypeThrift.WriteMutationLog) {
        // for writeMutationLogs, need to
        // consider if some peers may have
        // saved my log
        result = processResponses(request, responseCollector);
        result.noPeerSaveMyLog = (numRequestSent == result.numOfPeerNotSaveMutationLog);
      } else {
        result = new BroadcastResult();
      }

      result.broadcastSucceeded = false;

      if (hasSnpashotVersionException) {
        throw new SnapshotVersionMissMatchForMergeLogsException(result);
      } else {
        throw new FailedToSendBroadcastRequestsException(result);
      }
    }

    // count down the latch as the same times as the number of requests that were failed to send
    int numRequestsNotSent = eps.size() - numRequestSent;
    while (numRequestsNotSent > 0) {
      latch.countDown();
      numRequestsNotSent--;
    }

    // Wait until all responses have been received
    try {
      latch.await();
    } catch (InterruptedException e) {
      logger.info("interrupted while can't wait for all push request coming back");
    }

    // Process the responses
    // find all recoverable exceptions from the end points and update backoff data structure
    // accordingly
    BroadcastResult broadcastResult = processResponses(request, responseCollector);
    int sizeOfGoodResponses = broadcastResult.goodResponses.size();

    if (membership.isPresent() && mapEndPointToInstanceId.isPresent()) {
      broadcastResult.broadcastSucceeded = checkResultAccordingToMembership(
          responseCollector.getGoodOnes(),
          mapEndPointToInstanceId.get(), membership.get(), quorumSize);
    } else {
      broadcastResult.broadcastSucceeded = sizeOfGoodResponses >= quorumSize;
    }
    if (!broadcastResult.broadcastSucceeded) {
      if (request.getLogType() == BroadcastTypeThrift.WriteMutationLogs) { // Dead Code
        broadcastResult.noPeerSaveMyLog = (broadcastResult.numOfPeerNotSaveMutationLog
            == numRequestSent);
      }
      String exceptionString = null;
      if (broadcastResult.getExceptions() != null) {
        for (Entry<EndPoint, Throwable> entry : broadcastResult.getExceptions().entrySet()) {
          String tmp =
              "endpoint=" + entry.getKey() + ", exception class=" + entry.getValue().getClass()
                  .getSimpleName();
          if (exceptionString == null) {
            exceptionString = tmp;
          } else {
            exceptionString += ";" + tmp;
          }
        }
      }

      boolean hasSnapshotVersionException = false;
      logger.warn(
          "Only broadcast the request to {}, endpoints:{} (total is {}). Request: {}, no peer save "
              + "my log {}, BAD ones:{}, exception={}",
          sizeOfGoodResponses, eps, eps.size(), request, broadcastResult.noPeerSaveMyLog,
          responseCollector.getBadOnes(), exceptionString);
      if (hasSnapshotVersionException) {
        throw new SnapshotVersionMissMatchForMergeLogsException(broadcastResult);
      } else {
        throw new QuorumNotFoundException(broadcastResult);
      }
    }

    // Don't extend the lease because the broadcast request is usually sent by a primary
    // We don't extend primary lease any more. Whether a primary lease expires depends
    // on whether its peers (secondaries) have expired
    // if (segmentLeaseHandler != null) {
    // extending my lease
    // segmentLeaseHandler.extendMyLease();
    // }

    return broadcastResult;
  }

  /**
   * The function pushes a log to secondaries.
   *
   * @param quorumSize  if quorumSize is 0 or negative, then the function returns immediately after
   *                    sending out requests instead of waiting for responses.
   * @param exitEarlier when it is true, the function returns without waiting to receive all
   *                    responses as long as a (good/bad) quorum has reached By default, it is true
   */
  public BroadcastResult broadcast(SegmentLeaseHandler segmentLeaseHandler, int leaseSpanForPeer,
      BroadcastRequest request, long requestTimeout, int quorumSize, boolean exitEarlier,
      Collection<EndPoint> epsPassedIn, boolean backoffIfNeeded, long totalTimeout)
      throws QuorumNotFoundException, FailedToSendBroadcastRequestsException,
      SnapshotVersionMissMatchForMergeLogsException {
    Collection<EndPoint> eps = epsPassedIn;
    int maxBackoffTime = ((int) totalTimeout / 2) == 0 ? 1000 : ((int) totalTimeout / 2);
    BroadcastResult preResult = null;
    BroadcastResult curResult = null;
    long startTime = System.currentTimeMillis();
    int failureTimes = 0;
    do {
      logger.debug("quorumSize {}, eps size {}. \nPrev result {}, \ncurrent result {}", quorumSize,
          eps.size(),
          preResult, curResult);
      // reserve the previous result
      preResult = curResult;
      try {
        curResult = broadcast(request, requestTimeout, quorumSize, exitEarlier, eps);
        curResult.mergePreviousResult(preResult);
        // succeed and return
        break;
      } catch (SnapshotVersionMissMatchForMergeLogsException e) {
        throw e;
      } catch (FailedToSendBroadcastRequestsException e) {
        // don't need to retry
        throw e;
      } catch (QuorumNotFoundException e) {
        curResult = e.getBroadcastResult();
        if (backoffIfNeeded) {
          eps = getRetrableExceptionsFromResults(curResult);

          Map<EndPoint, BroadcastResponse> goodResponse = curResult.getGoodResponses();
          int numGoodResponse = goodResponse != null ? goodResponse.size() : 0;
          quorumSize -= numGoodResponse;

          Validate.isTrue(quorumSize > 0);
          // how long we have been running
          long howLongWeHaveRun = System.currentTimeMillis() - startTime;

          // merge the previous result with the current result
          curResult.mergePreviousResult(preResult);
          if (eps != null && eps.size() >= quorumSize && howLongWeHaveRun < totalTimeout) {
            int backoffTime = calculateBackoffTime(maxBackoffTime, failureTimes);
            failureTimes++;
            logger.warn("when broadcast the request adding backoff time {}, failureTime {}, "
                    + "howLongWeHaveRun {} and TotalTimeout {}", backoffTime, failureTimes,
                howLongWeHaveRun, totalTimeout);
            // sleep for a while
            curResult.incBackoffTimes();
            try {
              Thread.sleep(backoffTime);
            } catch (InterruptedException ie) {
              logger.error("caught exception", ie);
            }

            continue;
          }
        }
        // rethrow the exception because either we have retried or the condition to retry is not met
        throw new QuorumNotFoundException(curResult);
      }
    } while (true);

    return curResult;
  }

  public void asyncBroadcast(BroadcastRequest broadcastRequest, long requestTimeout, int quorumSize,
      Collection<EndPoint> eps, SegmentMembership membership,
      Map<EndPoint, InstanceId> mapEndPointToInstanceId,
      Map<EndPoint, AsyncMethodCallback<broadcast_call>> mapEndPointToCallback,
      AtomicBoolean finished)
      throws Exception {
    boolean notEnoughMembers = !checkResultAccordingToMembership(mapEndPointToInstanceId.keySet(),
        mapEndPointToInstanceId, membership, quorumSize);
    if (notEnoughMembers) {
      logger
          .warn("can't broadcast successfully because there are not enough members end points : {}",
              eps);
      BroadcastResult result = new BroadcastResult();
      result.broadcastSucceeded = false;
      finished.compareAndSet(false, true);
      throw new QuorumNotFoundException(result);
    }

    ResponseCollector<EndPoint, BroadcastResponse> responseCollector = new ResponseCollector<>();

    for (EndPoint endPointToBroadcast : eps) {
      try {
        // the timeout that is passed to the client might be larger than what the caller expected,
        // since this
        // function might have slept for a while because of backoff. I think it is ok to do so
        DataNodeService.AsyncIface asyncClient = getClient(endPointToBroadcast, requestTimeout);
        asyncClient.broadcast(broadcastRequest, mapEndPointToCallback.get(endPointToBroadcast));
        logger.debug("sent endpoint: {} a request {} , socket timeout: {}", endPointToBroadcast,
            broadcastRequest, requestTimeout);
      } catch (Exception e) {
        responseCollector.addClientSideThrowable(endPointToBroadcast, e);
        if (++exceptionCounter % EXCEPTION_RECORD_PER_NUM == 0) {
          logger.warn(
              "Caught an exception while sending a broadcast request {} to the end point {} this is"
                  + " {} such exceptions {}, exception occur times {}",
              broadcastRequest, endPointToBroadcast, responseCollector.numBadResponses(),
              e.getClass().getSimpleName(), exceptionCounter);
        }
      }
    }
    boolean tooManyBad = tooManyBadAccordinigToMembership(responseCollector.getBadOnes(),
        mapEndPointToInstanceId,
        membership, quorumSize);
    if (tooManyBad && finished.compareAndSet(false, true)) {
      logger.info(
          "Wanted to broadcast to {}, but it's impossible because there are already too many "
              + "requests failed {}",
          eps.size(), responseCollector);
      BroadcastResult result = new BroadcastResult();
      result.broadcastSucceeded = false;
      throw new FailedToSendBroadcastRequestsException(result);
    }
  }

  private Collection<EndPoint> getRetrableExceptionsFromResults(BroadcastResult broadcastResult) {
    if (broadcastResult == null || broadcastResult.exceptions == null || broadcastResult.exceptions
        .isEmpty()) {
      return null;
    }

    Collection<EndPoint> retrableEndPoints = new ArrayList<>();
    // find all recoverable exceptions from the end points and update backoff data structure
    // accordingly
    for (Map.Entry<EndPoint, Throwable> entry : broadcastResult.exceptions.entrySet()) {
      if (entry.getValue() instanceof ResourceExhaustedExceptionThrift) {
        // so far this is the only recoverable exception
        retrableEndPoints.add(entry.getKey());
        logger.debug("endpoint {} need to retry", entry.getKey());
      }
    }
    return retrableEndPoints;
  }

  private int calculateBackoffTime(int maxBackoffTime, int failureTimes) {
    // the max backoff time could be the half of the request timeout or 1 second if the half of
    // timeout is 0
    return Utils.getExponentialBackoffSleepTime(MIN_BACKOFF_UNIT_MS, failureTimes, maxBackoffTime);
  }

  private BroadcastResult processResponses(BroadcastRequest request,
      ResponseCollector<EndPoint, BroadcastResponse> responses) {
    BroadcastResult result = new BroadcastResult();
    result.goodResponses = new HashMap<>(responses.getGoodResponses());
    result.exceptions = new HashMap<>(responses.getServerSideThrowables());
    result.highestMembership = max(null, request.membership);

    // iterate the good responses
    for (Map.Entry<EndPoint, BroadcastResponse> entry : result.goodResponses.entrySet()) {
      BroadcastResponse goodResponse = entry.getValue();
      logger.trace("received a good response from endpoint {}. The response is {} ", entry.getKey(),
          goodResponse);
      result.highestMembership = max(entry.getValue().membership, result.highestMembership);
    }

    // iterate the bad responses
    SegmentMembershipThrift receivedMembership = null;
    for (Map.Entry<EndPoint, Throwable> entry : responses.serverSideExceptions.entrySet()) {
      Throwable exception = entry.getValue();
      if (exception instanceof PrimaryExistsExceptionThrift) {
        result.numPrimaryExistsExceptions++;
        PrimaryExistsExceptionThrift pe = (PrimaryExistsExceptionThrift) (exception);
        receivedMembership = pe.getMembership();
        if (pe.getDetail() != null) {
          logger.info("got a primary existing exception{} ", pe.getDetail());
        }
      } else if (exception instanceof LogIdTooSmallExceptionThrift) {
        result.numLogIdTooSmallExceptions++;
        LogIdTooSmallExceptionThrift lts = (LogIdTooSmallExceptionThrift) exception;
        receivedMembership = lts.getMembership();
        result.maxLogId = Math.max(result.maxLogId, lts.latestLogId);
        logger.info("got a log id too small exception {} ",
            lts.getDetail() != null ? lts.getDetail() : "");
      } else if (exception instanceof SegmentNotFoundExceptionThrift) {
        result.numSegmentNotFoundExceptions++;
        result.numOfPeerNotSaveMutationLog++;
      } else if (exception instanceof LeaseExtensionFrozenExceptionThrift) {
        result.numLeaseExtensionFrozenExceptions++;
        result.numOfPeerNotSaveMutationLog++;
      } else if (exception instanceof WriteMutationLogExceptionThrift) {
        WriteMutationLogExceptionThrift writeMutationLogException
            = (WriteMutationLogExceptionThrift) exception;
        if (writeMutationLogException.getReason() == WriteMutationLogFailReasonThrift.OutOfMemory) {
          result.numOfPeerNotSaveMutationLog++;
        }
      } else if (exception instanceof InvalidSegmentUnitStatusExceptionThrift) {
        InvalidSegmentUnitStatusExceptionThrift ise =
            (InvalidSegmentUnitStatusExceptionThrift) exception;
        receivedMembership = ise.getMembership();
        result.numOfPeerNotSaveMutationLog++;
      }  else {
        // using reflection to get the segment membership in the
        // exception if there is any
        receivedMembership = RequestResponseHelper.getSegmentMembershipFromThrowable(exception);
      }
      result.highestMembership = max(receivedMembership, result.highestMembership);
    }

    return result;
  }

  protected SegmentMembership max(SegmentMembershipThrift received,
      SegmentMembership currentHighest) {
    if (received == null && currentHighest == null) {
      return null;
    } else if (received == null) {
      return currentHighest;
    } else {
      // received is not null for sure
      // convert the received to the segment membership
      SegmentMembership receivedMembership = RequestResponseHelper
          .buildSegmentMembershipFrom(received)
          .getSecond();
      if (currentHighest == null) {
        return receivedMembership;
      } else {
        // Both are not null. compare them
        return receivedMembership.compareVersion(currentHighest) > 0 ? receivedMembership
            : currentHighest;
      }
    }
  }

  protected SegmentMembership max(SegmentMembershipThrift received,
      SegmentMembershipThrift currentHighest) {
    if (currentHighest == null) {
      return max(received, (SegmentMembership) null);
    } else {
      return max(received,
          RequestResponseHelper.buildSegmentMembershipFrom(currentHighest).getSecond());
    }
  }

  public DataNodeService.AsyncIface getClient(EndPoint primaryEndpoint, long connectionTimeout,
      long socketTimeout)
      throws GenericThriftClientFactoryException {
    return clientFactory
        .generateAsyncClient(primaryEndpoint, socketTimeout, (int) connectionTimeout);
  }

  public DataNodeService.AsyncIface getClient(EndPoint primaryEndpoint, long connectionTimeout)
      throws GenericThriftClientFactoryException {
    return clientFactory
        .generateAsyncClient(primaryEndpoint, connectionTimeout, (int) connectionTimeout);
  }

  public static class BroadcastResult {
    Map<EndPoint, BroadcastResponse> goodResponses;
    Map<EndPoint, Throwable> exceptions;

    int numPrimaryExistsExceptions = 0;
    int numLogIdTooSmallExceptions = 0;
    int numSegmentNotFoundExceptions = 0;
    int numLeaseExtensionFrozenExceptions = 0;
    int numOfPeerNotSaveMutationLog = 0;

    boolean broadcastSucceeded = true;
    boolean noPeerSaveMyLog = false;
    int backoffTimes = 0;

    SegmentMembership highestMembership;
    long maxLogId = 0;

    public Map<EndPoint, BroadcastResponse> getGoodResponses() {
      return goodResponses;
    }

    public void setGoodResponses(Map<EndPoint, BroadcastResponse> goodResponses) {
      this.goodResponses = goodResponses;
    }

    public Map<EndPoint, Throwable> getExceptions() {
      return exceptions;
    }

    // for testing purpose 
    public void setExceptions(Map<EndPoint, Throwable> exceptions) {
      this.exceptions = exceptions;
    }

    public int getNumPrimaryExistsExceptions() {
      return numPrimaryExistsExceptions;
    }

    public void setNumPrimaryExistsExceptions(int numPrimaryExistsExceptions) {
      this.numPrimaryExistsExceptions = numPrimaryExistsExceptions;
    }

    public int getNumLogIdTooSmallExceptions() {
      return numLogIdTooSmallExceptions;
    }

    public void setNumLogIdTooSmallExceptions(int numLogIdTooSmallExceptions) {
      this.numLogIdTooSmallExceptions = numLogIdTooSmallExceptions;
    }

    public int getNumSegmentNotFoundExceptions() {
      return numSegmentNotFoundExceptions;
    }

    public void setNumSegmentNotFoundExceptions(int numSegmentNotFoundExceptions) {
      this.numSegmentNotFoundExceptions = numSegmentNotFoundExceptions;
    }

    public int getNumLeaseExtensionFrozenExceptions() {
      return numLeaseExtensionFrozenExceptions;
    }

    public void setNumLeaseExtensionFrozenExceptions(int numLeaseExtensionFrozenExceptions) {
      this.numLeaseExtensionFrozenExceptions = numLeaseExtensionFrozenExceptions;
    }

    public SegmentMembership getHighestMembership() {
      return highestMembership;
    }

    public void setHighestMembership(SegmentMembership highestMembership) {
      this.highestMembership = highestMembership;
    }

    public boolean isBroadcastSucceeded() {
      return broadcastSucceeded;
    }

    public long getMaxLogId() {
      return maxLogId;
    }

    public void setMaxLogId(long maxLogId) {
      this.maxLogId = maxLogId;
    }

    public int getBackoffTimes() {
      return backoffTimes;
    }

    public void incBackoffTimes() {
      backoffTimes++;
    }

    public boolean isNoPeerSaveMyLog() {
      return noPeerSaveMyLog;
    }

    public void setNoPeerSaveMyLog(boolean noPeerSaveMyLog) {
      this.noPeerSaveMyLog = noPeerSaveMyLog;
    }

    public void eliminateOneGoodResponseMember(EndPoint endPoint, Throwable e) {
      goodResponses.remove(endPoint);
      exceptions.putIfAbsent(endPoint, e);
    }

    @Override
    public String toString() {
      return "BroadcastResult [goodResponses=" + goodResponses + ", exceptions=" + exceptions
          + ", numPrimaryExistsExceptions=" + numPrimaryExistsExceptions
          + ", numLogIdTooSmallExceptions="
          + numLogIdTooSmallExceptions + ", numSegmentNotFoundExceptions="
          + numSegmentNotFoundExceptions
          + ", numLeaseExtensionFrozenExceptions=" + numLeaseExtensionFrozenExceptions
          + ", numOfPeerNotSaveMutationLog=" + numOfPeerNotSaveMutationLog + ", broadcastSucceeded="
          + broadcastSucceeded + ", noPeerSaveMyLog=" + noPeerSaveMyLog + ", backoffTimes="
          + backoffTimes
          + ", highestMembership=" + highestMembership + ", maxLogId=" + maxLogId + "]";
    }

    public void mergePreviousResult(BroadcastResult other) {
      if (other == null) {
        return;
      }

      if (other.goodResponses != null) {
        if (goodResponses == null) {
          goodResponses = new HashMap<>(other.goodResponses);
        } else {
          for (Entry<EndPoint, BroadcastResponse> entry : other.goodResponses.entrySet()) {
            goodResponses.put(entry.getKey(), entry.getValue());
          }
        }
      }
      if (other.exceptions != null) {
        if (exceptions == null) {
          exceptions = new HashMap<>(other.exceptions);
        } else {
          for (Entry<EndPoint, Throwable> entry : other.exceptions.entrySet()) {
            if (goodResponses.get(entry.getKey()) == null) {
              // the other result indicates there is exception thrown from this endpoint
              // AND there is no good response
              exceptions.put(entry.getKey(), entry.getValue());
            }
          }
        }
      }

      // remove all exceptions that appear as good response in the current

      numPrimaryExistsExceptions += other.numPrimaryExistsExceptions;
      numLogIdTooSmallExceptions += other.numLogIdTooSmallExceptions;
      numSegmentNotFoundExceptions += other.numSegmentNotFoundExceptions;
      numLeaseExtensionFrozenExceptions += other.numLeaseExtensionFrozenExceptions;
      numOfPeerNotSaveMutationLog += other.numOfPeerNotSaveMutationLog;

      // when my result is not succeeded, the previous result should not succeed
      Validate.isTrue(broadcastSucceeded || !other.broadcastSucceeded);
      // don't merge broadcastSucceeded

      if (!noPeerSaveMyLog && other.noPeerSaveMyLog) {
        noPeerSaveMyLog = true;
      }

      maxLogId = Math.max(maxLogId, other.maxLogId);

      if (highestMembership != null) {
        if (highestMembership.compareTo(other.highestMembership) < 0) {
          highestMembership = other.highestMembership;
        }
      } else {
        highestMembership = other.highestMembership;
      }

      backoffTimes = Math.max(backoffTimes, other.backoffTimes);
    }
  }

  private static class BroadcastMethodCallbackWithMembership extends AbstractBroadcastCallback {
    private final CountDownLatch latch;
    private final boolean exitEarlier;
    private final int quorumSize;
    private final SegmentMembership membership;
    private final Map<EndPoint, InstanceId> mapEndPointToInstanceId;

    public BroadcastMethodCallbackWithMembership(
        ResponseCollector<EndPoint, BroadcastResponse> responseCollector,
        BroadcastRequest request, EndPoint endPoint, int totalSize, CountDownLatch latch,
        boolean exitEarlier,
        int quorumSize, SegmentMembership membership,
        Map<EndPoint, InstanceId> mapEndPointToInstanceId) {
      super(responseCollector, request, endPoint, totalSize);
      this.latch = latch;
      this.exitEarlier = exitEarlier;
      this.quorumSize = quorumSize;
      this.membership = membership;
      this.mapEndPointToInstanceId = mapEndPointToInstanceId;
    }

    @Override
    public void checkDone() {
      if (exitEarlier) {
        if (checkResultAccordingToMembership(responseCollector.getGoodOnes(),
            mapEndPointToInstanceId,
            membership, quorumSize)) {
          logger.debug("Got a good quorum. Bait out");
          wakeupMainThread(totalSize);
          return;
        }
        if (tooManyBadAccordinigToMembership(responseCollector.getBadOnes(),
            mapEndPointToInstanceId,
            membership, quorumSize)) {
          logger.info("it is impossible to get a good quorum any more. Bait out");
          wakeupMainThread(totalSize);
          return;
        }
      }
      latch.countDown();
    }

    private void wakeupMainThread(int totalSize) {
      for (int i = 0; i < totalSize; i++) {
        latch.countDown();
      }
    }
  }

  /**
   * The class collects responses and decides when to wake up the main thread.
   *
   * <p>There are 3 cases where we need to wake up the main thread 1. the number of good responses
   * received equals the quorum size 2. a stale membership exception is caught 3. the number of bad
   * responses received makes it impossible to receive quorum
   *
   */
  private static class BroadcastMethodCallback extends AbstractBroadcastCallback {
    private final CountDownLatch latch;
    private final boolean exitEarlier;
    private final int leaseSpanForPeer;

    private final int quorumSize;

    private final Optional<SegmentLeaseHandler> segmentLeaseHandler;

    public BroadcastMethodCallback(Optional<SegmentLeaseHandler> segmentLeaseHandler,
        int leaseSpanForPeer,
        BroadcastRequest request, EndPoint endPoint,
        ResponseCollector<EndPoint, BroadcastResponse> responseCollector, int totalSize,
        int quorumSize,
        boolean exitEarlier, CountDownLatch latch) {
      super(responseCollector, request, endPoint, totalSize);
      this.latch = latch;
      this.quorumSize = quorumSize;
      this.exitEarlier = exitEarlier;
      this.segmentLeaseHandler = segmentLeaseHandler;
      this.leaseSpanForPeer = leaseSpanForPeer;
    }

    public void processComplete(BroadcastResponse response) {
      Validate.isTrue(response.getRequestId() == request.getRequestId());
      if (segmentLeaseHandler.isPresent()) {
        segmentLeaseHandler.get()
            .extendPeerLease(leaseSpanForPeer, new InstanceId(response.getMyInstanceId()));
      }
      logger.debug("onComplete. The request is {}  The response is : {} ", request, response);

    }

    public void checkDone() {
      if (exitEarlier) {
        if (responseCollector.numGoodResponses() >= quorumSize) {
          logger.debug("Got a good quorum. Bait out");
          wakeupMainThread(totalSize);
          return;
        }
        if (isTooManyBad(totalSize, responseCollector.numBadResponses(), quorumSize)) {
          logger.info("it is impossible to get a good quorum any more. Bait out");
          wakeupMainThread(totalSize);
          return;
        }
      }
      latch.countDown();
    }

    private boolean isTooManyBad(int total, int numBads, int quorumSize) {
      if (total - numBads < quorumSize) {
        return true;
      } else {
        return false;
      }
    }

    private void wakeupMainThread(int totalSize) {
      for (int i = 0; i < totalSize; i++) {
        latch.countDown();
      }
    }
  }

}
