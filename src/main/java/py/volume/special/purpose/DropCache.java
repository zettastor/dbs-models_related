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

package py.volume.special.purpose;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.client.thrift.GenericThriftClientFactory;
import py.common.Constants;
import py.common.PyService;
import py.common.RequestIdBuilder;
import py.common.struct.EndPoint;
import py.dih.client.DihClientFactory;
import py.exception.GenericThriftClientFactoryException;
import py.instance.Instance;
import py.instance.InstanceStatus;
import py.thrift.datanode.service.DataNodeService;
import py.thrift.share.GetConfigurationsRequest;
import py.thrift.share.GetConfigurationsResponse;
import py.thrift.share.SetConfigurationsRequest;
import py.thrift.share.SetConfigurationsResponse;

public class DropCache {
  private static final boolean ENABLE = false;
  private static final String dihHost = "localhost";
  private static final String CONFIG_STRING = "skippingCacheAndCorruptData";
  private static final Logger logger = LoggerFactory.getLogger(DropCache.class);
  private static final GenericThriftClientFactory<DataNodeService.Iface> clientFactory =
      GenericThriftClientFactory.create(DataNodeService.Iface.class);
  private static final DihClientFactory dihClientFactory = new DihClientFactory(1);

  /**
   * Set drop.
   *
   * @return Green if none dropped, Red if all dropped, Yellow if partly dropped
   * @throws TimeoutException if timed out before all finished
   */
  public static Color setDrop(long timeoutMs, boolean isDrop)
      throws TimeoutException {
    if (!ENABLE) {
      return Color.Green;
    }

    final long startTime = System.currentTimeMillis();

    Set<Instance> datanodes = getAllOkDataNodes(dihHost, timeoutMs);

    for (Instance datanode : datanodes) {
      DataNodeService.Iface client = getClient(startTime, timeoutMs, datanode.getEndPoint());

      boolean success = false;

      SetConfigurationsRequest request = new SetConfigurationsRequest(RequestIdBuilder.get(),
          Collections.singletonMap(CONFIG_STRING, Boolean.valueOf(isDrop).toString()));

      while (!timedOut(startTime, timeoutMs)) {
        try {
          SetConfigurationsResponse response = client.setConfigurations(request);
          logger.warn("response is {}", response);
          success = true;
          break;
        } catch (TException e) {
          logger.warn("set configuration failed for {}", datanode, e);
          try {
            Thread.sleep(100);
          } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
          }
        }
      }

      if (!success) {
        if (timedOut(startTime, timeoutMs)) {
          throw new TimeoutException();
        } else {
          logger.error("not timeout but failed");
          throw new IllegalStateException();
        }
      }

    }

    Color color = queryState(timeoutMs - (System.currentTimeMillis() - startTime));

    Color expected = isDrop ? Color.Red : Color.Green;

    if (color != expected) {
      logger.warn("expected {} but {}, retry...", expected, color);
      return setDrop(timeoutMs - (System.currentTimeMillis() - startTime), isDrop);
    } else {
      return color;
    }

  }

  /**
   * Query the current state.
   *
   * @return Green if none dropped, Red if all dropped, Yellow if partly dropped
   * @throws TimeoutException if timed out before all finished
   */
  public static Color queryState(long timeoutMs) throws TimeoutException {
    if (!ENABLE) {
      return Color.Green;
    }

    final long startTime = System.currentTimeMillis();

    Set<Instance> datanodes = getAllOkDataNodes(dihHost, timeoutMs);

    int droppedCount = 0;

    for (Instance datanode : datanodes) {
      DataNodeService.Iface client = getClient(startTime, timeoutMs, datanode.getEndPoint());

      boolean success = false;

      while (System.currentTimeMillis() - startTime < timeoutMs) {
        GetConfigurationsRequest request = new GetConfigurationsRequest(RequestIdBuilder.get());
        request.setKeys(Collections.singleton(CONFIG_STRING));
        try {
          GetConfigurationsResponse response = client.getConfigurations(request);
          Map<String, String> result = response.getResults();

          if (result.size() != 1) {
            logger.error("wrong result {}", response);
            throw new IllegalStateException();
          }

          String value = result.values().iterator().next();

          boolean isDropped;

          if (value.equals(Boolean.TRUE.toString())) {
            isDropped = true;
          } else if (value.equals(Boolean.FALSE.toString())) {
            isDropped = false;
          } else {
            throw new IllegalStateException("unknown boolean " + value);
          }

          if (isDropped) {
            droppedCount++;
          }

          success = true;
          break;

        } catch (TException e) {
          logger.warn("get configuration from {} failed", datanode, e);
          try {
            Thread.sleep(100);
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
          }
        }

      }

      if (!success) {
        if (timedOut(startTime, timeoutMs)) {
          throw new TimeoutException();
        } else {
          logger.error("not timed out but failed");
          throw new IllegalStateException();
        }
      }

    }

    if (droppedCount == datanodes.size()) {
      return Color.Red;
    } else if (droppedCount == 0) {
      return Color.Green;
    } else {
      logger.warn("dropped count {}, total {}", droppedCount, datanodes.size());
      return Color.Yellow;
    }

  }

  private static DataNodeService.Iface getClient(long startTime, long timeoutMs, EndPoint ep)
      throws TimeoutException {
    DataNodeService.Iface client = null;

    while (!timedOut(startTime, timeoutMs)) {
      try {
        client = clientFactory.generateSyncClient(ep);
        break;
      } catch (GenericThriftClientFactoryException e) {
        logger.error("can't generate data node client", e);
        try {
          Thread.sleep(200);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
        }
      }
    }

    if (client == null) {
      if (timedOut(startTime, timeoutMs)) {
        logger.warn("timed out generating data node client for {}", ep);
        throw new TimeoutException();
      } else {
        logger.error("data node client is null for {}", ep);
        throw new IllegalStateException();
      }
    } else {
      return client;
    }
  }

  private static boolean timedOut(long start, long timeoutMs) {
    return System.currentTimeMillis() - start > timeoutMs;
  }

  public static Set<Instance> getAllOkDataNodes(String dihHost, long timeout)
      throws TimeoutException {
    final long startTime = System.currentTimeMillis();

    while (!timedOut(startTime, timeout)) {
      try {
        return dihClientFactory.build(new EndPoint(dihHost, Constants.DIH_PORT))
            .getInstances(PyService.DATANODE.getServiceName(), InstanceStatus.HEALTHY);
      } catch (Exception e) {
        logger.warn("catch an exception", e);
      }
      logger.warn("fail to get all instance and retry");
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    throw new TimeoutException("fail to get all instances in 100s");
  }

  public enum Color {
    Green, Red, Yellow
  }
}
