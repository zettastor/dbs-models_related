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

package py.membership;

public class SecondariesCountInfo {
  private int fetchCount = 0;
  private int checkCount = 0;

  public SecondariesCountInfo() {
  }

  public int getFetchCount() {
    return fetchCount;
  }

  public void setFetchCount(int fetchCount) {
    this.fetchCount = fetchCount;
  }

  public int getCheckCount() {
    return checkCount;
  }

  public void setCheckCount(int checkCount) {
    this.checkCount = checkCount;
  }

  public int addFetchCount() {
    return fetchCount++;
  }

  public int addCheckCount() {
    return checkCount++;
  }

  public int allSecondariesCount() {
    return getCheckCount() + getFetchCount();
  }

  @Override
  public String toString() {
    return "SecondariesCountInfo{"
        + "fetchCount=" + fetchCount
        + ", checkCount=" + checkCount
        + '}';
  }

}
