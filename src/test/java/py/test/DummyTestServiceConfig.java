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
