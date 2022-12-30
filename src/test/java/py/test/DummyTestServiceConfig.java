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

public class DummyTestServiceConfig {
  private static final int DEFAULT_SERVICE_PORT = 22222;
  private int sendMaxFrameSize;
  private int receiveMaxFromeSize;
  private boolean blocking;
  private int latency;
  private boolean randomizeLatency;
  private int servicePort;
  private int reponseSize;
  private Object returnObject;
  private int networkTimeout;
  private int maxNumThreads;
  private int numWorkerThreads;

  public static DummyTestServiceConfig create() {
    DummyTestServiceConfig cfg = new DummyTestServiceConfig();
    cfg.setReceiveMaxFromeSize(16 * 1024 * 1024);
    cfg.setSendMaxFrameSize(16 * 1024 * 1024);
    cfg.setBlocking(true);
    cfg.setLatency(0);
    cfg.setServicePort(DEFAULT_SERVICE_PORT);
    cfg.setRandomizeLatency(false);
    cfg.setReponseSize(0);
    cfg.setReturnObject(null);
    cfg.setNetworkTimeout(3000);
    cfg.setMaxNumThreads(Runtime.getRuntime().availableProcessors() * 4);
    cfg.setNumWorkerThreads(Runtime.getRuntime().availableProcessors());
    return cfg;
  }

  public int getSendMaxFrameSize() {
    return sendMaxFrameSize;
  }

  public DummyTestServiceConfig setSendMaxFrameSize(int sendMaxFrameSize) {
    this.sendMaxFrameSize = sendMaxFrameSize;
    return this;
  }

  public int getReceiveMaxFromeSize() {
    return receiveMaxFromeSize;
  }

  public DummyTestServiceConfig setReceiveMaxFromeSize(int receiveMaxFromeSize) {
    this.receiveMaxFromeSize = receiveMaxFromeSize;
    return this;
  }

  public boolean isBlocking() {
    return blocking;
  }

  public DummyTestServiceConfig setBlocking(boolean blocking) {
    this.blocking = blocking;
    return this;
  }

  public int getLatency() {
    return latency;
  }

  public DummyTestServiceConfig setLatency(int latency) {
    this.latency = latency;
    return this;
  }

  public boolean isRandomizeLatency() {
    return randomizeLatency;
  }

  public DummyTestServiceConfig setRandomizeLatency(boolean randomizeLatency) {
    this.randomizeLatency = randomizeLatency;
    return this;
  }

  public int getMaxNumThreads() {
    return maxNumThreads;
  }

  public void setMaxNumThreads(int maxNumThreads) {
    this.maxNumThreads = maxNumThreads;
  }

  public int getServicePort() {
    return servicePort;
  }

  public DummyTestServiceConfig setServicePort(int servicePort) {
    this.servicePort = servicePort;
    return this;
  }

  public int getReponseSize() {
    return reponseSize;
  }

  public DummyTestServiceConfig setReponseSize(int reponseSize) {
    this.reponseSize = reponseSize;
    return this;
  }

  public Object getReturnObject() {
    return returnObject;
  }

  public void setReturnObject(Object returnObject) {
    this.returnObject = returnObject;
  }

  public int getNetworkTimeout() {
    return networkTimeout;
  }

  public void setNetworkTimeout(int networkTimeout) {
    this.networkTimeout = networkTimeout;
  }

  public int getNumWorkerThreads() {
    return numWorkerThreads;
  }

  public void setNumWorkerThreads(int numWorkerThreads) {
    this.numWorkerThreads = numWorkerThreads;
  }

}
