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

package py.datanode.client;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Collecting all async results.
 */
public class ResponseCollector<K, T> {
  Map<K, T> responses = new ConcurrentHashMap<>();
  Map<K, Throwable> serverSideExceptions = new ConcurrentHashMap<>();
  Map<K, Throwable> clientSideExceptions = new ConcurrentHashMap<>();

  public void addGoodResponse(K ep, T t) {
    responses.put(ep, t);
  }

  public void addServerSideThrowable(K ep, Throwable t) {
    serverSideExceptions.put(ep, t);
  }

  public void addClientSideThrowable(K ep, Throwable t) {
    clientSideExceptions.put(ep, t);
  }

  public Map<K, T> getGoodResponses() {
    return responses;
  }

  public Map<K, Throwable> getServerSideThrowables() {
    return serverSideExceptions;
  }

  public Map<K, Throwable> getClientSideThrowables() {
    return clientSideExceptions;
  }

  public Collection<K> getGoodOnes() {
    return new ArrayList<>(responses.keySet());
  }

  public Collection<K> getBadOnes() {
    List<K> badOnes = new ArrayList<>();
    badOnes.addAll(serverSideExceptions.keySet());
    badOnes.addAll(clientSideExceptions.keySet());
    return badOnes;
  }

  public int numGoodResponses() {
    return responses.size();
  }

  public int numBadResponses() {
    return serverSideExceptions.size() + clientSideExceptions.size();
  }

  @Override
  public String toString() {
    return "ResponseCollector{" + "responses=" + responses + ", serverSideExceptions="
        + serverSideExceptions
        + ", clientSideExceptions=" + clientSideExceptions + '}';
  }
}
