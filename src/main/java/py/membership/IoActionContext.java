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

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import org.apache.commons.lang3.Validate;
import py.instance.InstanceId;

public class IoActionContext {
  // NO io member which is down will be added to this list
  private Set<IoMember> ioMembers;

  private SegmentForm segmentForm;
  private boolean zombieRequest;

  private boolean unstablePrimaryWrite;

  private boolean resendDirectly;

  // mark segment membership when io come, use it to compare with newest membership
  private SegmentMembership membershipWhenIoCome;

  private boolean metReadDownSecondary;

  private int totalWriteCount;
  private Set<InstanceId> netUnHealthyInstanceId;

  public IoActionContext() {
    this.ioMembers = new HashSet<>();
    this.zombieRequest = false;
    this.unstablePrimaryWrite = false;
    this.resendDirectly = false;
    this.metReadDownSecondary = false;
    this.netUnHealthyInstanceId = new HashSet();
  }

  public boolean isResendDirectly() {
    return resendDirectly;
  }

  public void setResendDirectly(boolean resendDirectly) {
    this.resendDirectly = resendDirectly;
  }

  public Set<IoMember> getIoMembers() {
    return ioMembers;
  }

  public Set<IoMember> getRealReaders() {
    Set<IoMember> realReaders = new HashSet<>();
    for (IoMember ioMember : ioMembers) {
      if (!ioMember.isCheckRead()) {
        realReaders.add(ioMember);
      }
    }
    if (realReaders.isEmpty()) {
      Validate.isTrue(false, "can not be empty for read");
    }
    return realReaders;
  }

  public Set<IoMember> getFetchReader() {
    Set<IoMember> fetchReader = new HashSet<>();
    for (IoMember ioMember : ioMembers) {
      if (ioMember.isFetchRead()) {
        fetchReader.add(ioMember);
      }
    }
    if (fetchReader.size() > 1) {
      Validate.isTrue(false, "fetch can not more than one:" + fetchReader);
    }
    return fetchReader;
  }

  public Set<IoMember> getCheckReaders() {
    Set<IoMember> checkReaders = new HashSet<>();
    for (IoMember ioMember : ioMembers) {
      if (ioMember.isCheckRead()) {
        checkReaders.add(ioMember);
      }
    }
    return checkReaders;
  }

  public boolean isZombieRequest() {
    return zombieRequest;
  }

  public void setZombieRequest(boolean zombieRequest) {
    this.zombieRequest = zombieRequest;
  }

  public SegmentForm getSegmentForm() {
    return segmentForm;
  }

  public void setSegmentForm(SegmentForm segmentForm) {
    this.segmentForm = segmentForm;
  }

  public boolean isMetReadDownSecondary() {
    return metReadDownSecondary;
  }

  public void setMetReadDownSecondary(boolean metReadDownSecondary) {
    this.metReadDownSecondary = metReadDownSecondary;
  }

  public void addIoMember(IoMember ioMember) {
    this.ioMembers.add(ioMember);
  }

  public void doNotNeedCheckRead() {
    Iterator<IoMember> it = ioMembers.iterator();
    while (it.hasNext()) {
      IoMember ioMember = it.next();
      if (ioMember.isCheckRead()) {
        it.remove();
      }
    }
  }

  /**
   * check if has primary, means primary is not down, can do writing or reading.
   */
  public boolean isPrimaryDown() {
    for (IoMember ioMember : ioMembers) {
      MemberIoStatus memberIoStatus = ioMember.getMemberIoStatus();
      if (memberIoStatus.isPrimary()) {
        return false;
      }
    }
    return true;
  }

  /**
   * even if has one secondary, still means secondary is not down, cuz only PSJ or PSI(PS) or
   * PSA(TPS) care about secondary status.
   */
  public boolean isSecondaryDown() {
    for (IoMember ioMember : ioMembers) {
      MemberIoStatus memberIoStatus = ioMember.getMemberIoStatus();
      if (memberIoStatus.isSecondary()) {
        return false;
      }
    }
    return true;
  }

  public boolean isJoiningSecondaryDown() {
    for (IoMember ioMember : ioMembers) {
      MemberIoStatus memberIoStatus = ioMember.getMemberIoStatus();
      if (memberIoStatus.isJoiningSecondary()) {
        return false;
      }
    }
    return true;
  }

  public boolean gotTempPrimary() {
    for (IoMember ioMember : ioMembers) {
      MemberIoStatus memberIoStatus = ioMember.getMemberIoStatus();
      if (memberIoStatus.isTempPrimary()) {
        return true;
      }
    }
    return false;
  }

  public SegmentMembership getMembershipWhenIoCome() {
    return membershipWhenIoCome;
  }

  public void setMembershipWhenIoCome(SegmentMembership membershipWhenIoCome) {
    this.membershipWhenIoCome = membershipWhenIoCome;
  }

  public boolean isUnstablePrimaryWrite() {
    return unstablePrimaryWrite;
  }

  public void setUnstablePrimaryWrite(boolean unstablePrimaryWrite) {
    this.unstablePrimaryWrite = unstablePrimaryWrite;
  }

  public int getTotalWriteCount() {
    return totalWriteCount;
  }

  public void setTotalWriteCount(int totalWriteCount) {
    this.totalWriteCount = totalWriteCount;
  }

  public void releaseReference() {
    // when PS(S was marked down status), fetch read has response, but next check read will throw
    // null pointer
  }

  @Override
  public String toString() {
    return "IOActionContext{" + "ioMembers=" + ioMembers + ", segmentForm=" + segmentForm
        + ", zombieRequest="
        + zombieRequest + ", unstablePrimaryWrite=" + unstablePrimaryWrite + ", resendDirectly="
        + resendDirectly + ", membershipWhenIOCome=" + membershipWhenIoCome
        + ", metReadDownSecondary="
        + metReadDownSecondary + '}';
  }

  public Set<InstanceId> getNetUnHealthyInstanceId() {
    return netUnHealthyInstanceId;
  }

  public void addUnHealthyInstanceId(InstanceId instanceId) {
    netUnHealthyInstanceId.add(instanceId);
  }
}
