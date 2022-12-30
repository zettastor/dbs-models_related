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
