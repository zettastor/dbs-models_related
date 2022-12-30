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

package py.archive.segment;

import java.util.Collection;
import java.util.Set;
import py.common.struct.Pair;
import py.exception.LeaseExtensionFrozenException;
import py.instance.InstanceId;

public interface SegmentLeaseHandler {
  /**
   * Extend the lease by using the default lease span set by startMyLease().
   *
   * @return true if extending succeeded. Otherwise false when the lease has expired
   * @throws LeaseExtensionFrozenException when the lease is hold
   */
  public boolean extendMyLease() throws LeaseExtensionFrozenException;

  /**
   * Extend the lease by using the specify the lease span.
   *
   * @return true if extending succeeded. Otherwise false when the lease has expired
   */
  public boolean extendMyLease(long lease) throws LeaseExtensionFrozenException;

  /**
   * start the lease with a new lease span. If the current lease has expired, it will be cleared.
   * This function always succeeds.
   */
  public void startMyLease(long lease);

  public void clearMyLease();

  public boolean myLeaseExpired();

  public boolean myLeaseExpired(SegmentUnitStatus status);

  /**
   * The function is used by the primary to check if its lease has been expired The difference
   * between this function from myLeaseExpired() is that other than checking if my lease has
   * expired, it also checks if leases of the majority peers have expired. If both of them expired
   * then return true, otherwise return false.
   *
   */
  public Pair<Boolean, Set<InstanceId>> primaryLeaseExpired();

  /**
   * Used by the primary to add leases for all secondaries. If any peer leases have expired, its
   * expiration status will be cleared.
   *
   * <p>This function always succeeds.
   */
  public void startPeerLeases(long leaseSpan, Collection<InstanceId> peers);

  public boolean extendPeerLease(long leaseSpan, InstanceId peer);

  public void clearPeerLeases();

  public void startPeerLease(long leaseSpan, InstanceId peer);
}
