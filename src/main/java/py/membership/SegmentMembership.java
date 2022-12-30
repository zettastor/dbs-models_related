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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.segment.SegmentVersion;
import py.exception.InvalidMembershipException;
import py.instance.InstanceId;
import py.volume.VolumeType;

/**
 * This class stores the membership of a segment group.
 *
 * <p>The class itself is not thread safe. Whoever calls this class has to ensure the exclusive
 * access to this class.
 *
 * <p>This class is immutable. When the segment membership is changed, a new membership instance
 * has
 * to be created. Making it immutable make it easy to manager the membership because most of time
 * primary and secondaries change simultaneously.
 *
 * <p>The primary of a membership should not be null while secondaries, arbiters,
 * inactiveSecondaries, and joiningSecondaries can be null
 *
 * <p>A segment unit's membership changes are shown below: removed A secondary ==> inactive
 * secondary =======> gone
 *
 * <p>added A joining unit =========> a joiningSecondary =====> secondary || ||===> inactive
 * secondary
 *
 * <p>Primary, Secondaries and joining Secondaries can be called alive members. Secondaries and
 * joining Secondaries can be called alive secondaries.
 *
 */
public class SegmentMembership {
  private static final Logger logger = LoggerFactory.getLogger(SegmentMembership.class);
  // for serialize null pointer value
  private static final int null_value = -1;
  private final SegmentVersion segmentVersion;
  // Primary host's namePyAbstractClientChannel
  private final InstanceId primary;
  private final InstanceId tempPrimary;
  // Secondaries hosts' names. Secondary set could be empty because there could be a primary and an
  // arbiter
  // TODO: instead of using lots of sets, we can use one set to store secondaries, and
  // one byte to indicate the secondary's status: arbiter, inactive or joining.
  // when a secondary changes from active to inactive, we only need to change its status
  private final Set<InstanceId> secondaries;
  private final Set<InstanceId> arbiters;
  private final Set<InstanceId> inactiveSecondaries;
  private final Set<InstanceId> joiningSecondaries;

  // these two are for re-balancing support
  private final InstanceId primaryCandidate;
  private final InstanceId secondaryCandidate;

  // This is always false in secondaries.
  // And in primary, if quorum members has updated their membership to this, this will be set to
  // true.
  @JsonIgnore
  private boolean quorumUpdated = false;

  // TODO This should not be here because SegmentMembership is an immutable class.
  @JsonIgnore
  private Map<InstanceId, MemberIoStatus> memberIoStatusMap;

  public SegmentMembership(final InstanceId primary, final Collection<InstanceId> secondaries) {
    this(new SegmentVersion(0, 0), primary, null, secondaries, null, null, null, null, null);
  }

  public SegmentMembership(final InstanceId primary, final Collection<InstanceId> secondaries,
      final Collection<InstanceId> arbiters) {
    this(new SegmentVersion(0, 0), primary, null, secondaries, arbiters, null, null, null, null);
  }

  public SegmentMembership(final SegmentVersion version, final InstanceId primary,
      final Collection<InstanceId> secondaries) {
    this(version, primary, secondaries, null, null, null);
  }

  public SegmentMembership(final SegmentVersion version, final InstanceId primary,
      final Collection<InstanceId> secondaries, final Collection<InstanceId> arbiters) {
    this(version, primary, secondaries, arbiters, null, null);
  }

  public SegmentMembership(SegmentVersion segmentVersion, InstanceId primary,
      Collection<InstanceId> secondaries,
      Collection<InstanceId> arbiters, Collection<InstanceId> inactiveSecondaries,
      Collection<InstanceId> joiningSecondaries) {
    this(segmentVersion, primary, null, secondaries, arbiters, inactiveSecondaries,
        joiningSecondaries, null, null);
  }

  public SegmentMembership(@JsonProperty("segmentVersion") final SegmentVersion version,
      @JsonProperty("primary") final InstanceId primary,
      @JsonProperty("tempPrimary") final InstanceId tempPrimary,
      @JsonProperty("secondaries") final Collection<InstanceId> secondaries,
      @JsonProperty("arbiters") final Collection<InstanceId> arbiters,
      @JsonProperty("inactiveSecondaries") final Collection<InstanceId> inactiveSecondaries,
      @JsonProperty("joiningSecondaries") final Collection<InstanceId> joiningSecondaries,
      @JsonProperty("secondaryCandidate") final InstanceId secondaryCandidate,
      @JsonProperty("primaryCandidate") final InstanceId primaryCandidate) {
    this.segmentVersion = version;
    this.primary = primary;

    if (tempPrimary == null || tempPrimary.getId() == 0) {
      this.tempPrimary = null;
    } else {
      this.tempPrimary = tempPrimary;
    }

    if (secondaryCandidate == null || secondaryCandidate.getId() == 0) {
      this.secondaryCandidate = null;
    } else {
      this.secondaryCandidate = secondaryCandidate;
    }

    if (primaryCandidate == null || primaryCandidate.getId() == 0) {
      this.primaryCandidate = null;
    } else {
      this.primaryCandidate = primaryCandidate;
    }

    // secondaries could be null
    if (secondaries != null) {
      this.secondaries = new HashSet<>(secondaries);
      Validate.isTrue(!secondaries.contains(primary),
          "this should not happen. the secondaries should not contain the specified primary");
    } else {
      this.secondaries = new HashSet<>();
    }

    if (arbiters != null) {
      this.arbiters = new HashSet<>(arbiters);
      Validate.isTrue(!arbiters.contains(primary),
          "this should not happen. the arbiter should not contain the specified primary");
    } else {
      this.arbiters = new HashSet<>();
    }

    if (inactiveSecondaries != null) {
      this.inactiveSecondaries = new HashSet<>(inactiveSecondaries);
      Validate.isTrue(!inactiveSecondaries.contains(primary),
          "this should not happen. the inactiveSecondaries should not contain the specified "
              + "primary");
    } else {
      this.inactiveSecondaries = new HashSet<>();
    }

    if (joiningSecondaries != null) {
      this.joiningSecondaries = new HashSet<>(joiningSecondaries);
      Validate.isTrue(!joiningSecondaries.contains(primary),
          "this should not happen. the joiningSecondaries should not contain the specified"
              + " primary");
    } else {
      this.joiningSecondaries = new HashSet<>();
    }

    quorumUpdated =
        this.secondaries.isEmpty() && this.arbiters.isEmpty() && this.joiningSecondaries.isEmpty();

    this.memberIoStatusMap = new ConcurrentHashMap<>();
  }

  public SegmentMembership(SegmentMembership copyFrom) {
    this.segmentVersion = new SegmentVersion(copyFrom.getSegmentVersion());
    this.primary = new InstanceId(copyFrom.getPrimary());
    this.tempPrimary = copyFrom.tempPrimary == null ? null : new InstanceId(copyFrom.tempPrimary);
    this.secondaries = new HashSet<>(copyFrom.secondaries);
    this.arbiters = new HashSet<>(copyFrom.arbiters);
    this.inactiveSecondaries = new HashSet<>(copyFrom.inactiveSecondaries);
    this.joiningSecondaries = new HashSet<>(copyFrom.joiningSecondaries);
    this.memberIoStatusMap = new ConcurrentHashMap<>(copyFrom.memberIoStatusMap);
    this.secondaryCandidate =
        copyFrom.secondaryCandidate == null ? null : new InstanceId(copyFrom.secondaryCandidate);
    this.primaryCandidate =
        copyFrom.primaryCandidate == null ? null : new InstanceId(copyFrom.primaryCandidate);
  }

  public static SegmentMembership deserializeFromObjectMapperContent(String value) {
    logger.debug("segment member ship from json value is {}", value);
    String[] fields = value.split(",");
    Validate.isTrue(10 == fields.length, "parse from json value failed, fields num not enough",
        fields.length);
    final int epoch = Integer.valueOf(fields[0]).intValue();
    final int generation = Integer.valueOf(fields[1]).intValue();
    final InstanceId jsonPrimary = Long.valueOf(fields[2]).longValue() == null_value
        ? null
        : new InstanceId(Long.valueOf(fields[2]).longValue());
    final InstanceId jsonTempPrimary = Long.valueOf(fields[3]).longValue() == null_value
        ? null
        : new InstanceId(Long.valueOf(fields[3]).longValue());

    /// secondaries
    Set<InstanceId> jsonSecondaries = new HashSet<>();
    String[] subFields;
    if (!fields[4].equals("")) {
      subFields = fields[4].split(":");
      for (String subField : subFields) {
        InstanceId jsonItem = new InstanceId(Long.valueOf(subField).longValue());
        jsonSecondaries.add(jsonItem);
      }
    }

    /// arbiters
    Set<InstanceId> jsonArbiters = new HashSet<>();
    if (!fields[5].equals("")) {
      subFields = fields[5].split(":");
      for (String subField : subFields) {
        InstanceId jsonItem = new InstanceId(Long.valueOf(subField).longValue());
        jsonArbiters.add(jsonItem);
      }
    }

    /// inactiveSecondaries
    Set<InstanceId> jsonInactiveSecondaries = new HashSet<>();
    if (!fields[6].equals("")) {
      subFields = fields[6].split(":");
      for (String subField : subFields) {
        InstanceId jsonItem = new InstanceId(Long.valueOf(subField).longValue());
        jsonInactiveSecondaries.add(jsonItem);
      }
    }

    // joiningSecondaries
    Set<InstanceId> jsonJoiningSecondaries = new HashSet<>();
    if (!fields[7].equals("")) {
      subFields = fields[7].split(":");
      for (String subField : subFields) {
        InstanceId jsonItem = new InstanceId(Long.valueOf(subField).longValue());
        jsonJoiningSecondaries.add(jsonItem);
      }
    }

    InstanceId jsonPrimaryCandidate = Long.valueOf(fields[8]).longValue() == null_value
        ? null : new InstanceId(Long.valueOf(fields[8]).longValue());

    InstanceId jsonSecondaryCandidate = Long.valueOf(fields[9]).longValue() == null_value
        ? null : new InstanceId(Long.valueOf(fields[9]).longValue());

    SegmentMembership segmentMembership = new SegmentMembership(
        new SegmentVersion(epoch, generation),
        jsonPrimary,
        jsonTempPrimary,
        jsonSecondaries,
        jsonArbiters,
        jsonInactiveSecondaries,
        jsonJoiningSecondaries,
        jsonSecondaryCandidate,
        jsonPrimaryCandidate
    );

    return segmentMembership;
  }

  public SegmentVersion getSegmentVersion() {
    return segmentVersion;
  }

  public boolean contain(InstanceId id) {
    Validate.notNull(id);
    return id.equals(primary) || secondaries.contains(id) || arbiters.contains(id)
        || inactiveSecondaries
        .contains(id) || joiningSecondaries.contains(id) || id.equals(secondaryCandidate);
  }

  public boolean contain(long id) {
    return contain(new InstanceId(id));
  }

  public boolean isPrimary(InstanceId id) {
    return id != null && id.equals(primary);
  }

  public boolean isTempPrimary(InstanceId id) {
    return id != null && id.equals(tempPrimary);
  }

  // check whether id is one of secondaries or joining secondaries or arbiter
  public boolean isAliveSecondaries(InstanceId id) {
    return isSecondary(id) || isJoiningSecondary(id) || isArbiter(id);
  }

  public boolean isSecondary(InstanceId id) {
    return id != null && secondaries.contains(id);
  }

  public boolean isArbiter(InstanceId id) {
    return id != null && arbiters.contains(id);
  }

  public boolean isJoiningSecondary(InstanceId id) {
    return id != null && joiningSecondaries.contains(id);
  }

  public boolean isInactiveSecondary(InstanceId id) {
    return id != null && inactiveSecondaries.contains(id);
  }

  public InstanceId getPrimary() {
    return primary;
  }

  public InstanceId getTempPrimary() {
    return tempPrimary;
  }

  public InstanceId getSecondaryCandidate() {
    return secondaryCandidate;
  }

  public InstanceId getPrimaryCandidate() {
    return primaryCandidate;
  }

  public boolean isSecondaryCandidate(InstanceId id) {
    return id != null && id.equals(secondaryCandidate);
  }

  public boolean isPrimaryCandidate(InstanceId id) {
    return id != null && id.equals(primaryCandidate);
  }

  public Set<InstanceId> getSecondaries() {
    return new HashSet<>(secondaries);
  }

  public Set<InstanceId> getArbiters() {
    return arbiters;
  }

  public Set<InstanceId> getInactiveSecondaries() {
    return inactiveSecondaries;
  }

  public Set<InstanceId> getJoiningSecondaries() {
    return joiningSecondaries;
  }

  @JsonIgnore
  public Set<InstanceId> getAllSecondaries() {
    Set<InstanceId> allSecondaries = new HashSet<>();
    allSecondaries.addAll(secondaries);
    allSecondaries.addAll(arbiters);
    allSecondaries.addAll(inactiveSecondaries);
    allSecondaries.addAll(joiningSecondaries);
    return allSecondaries;
  }

  @JsonIgnore
  public Set<InstanceId> getAliveSecondariesWithoutArbitersAndCandidate() {
    Set<InstanceId> allSecondariesWithoutArbiters = new HashSet<>();
    allSecondariesWithoutArbiters.addAll(secondaries);
    allSecondariesWithoutArbiters.addAll(joiningSecondaries);
    return allSecondariesWithoutArbiters;
  }

  @JsonIgnore
  public Set<InstanceId> getSecondariesAndArbiters() {
    Set<InstanceId> secondariesAndArbiters = new HashSet<>();
    secondariesAndArbiters.addAll(secondaries);
    secondariesAndArbiters.addAll(arbiters);
    return secondariesAndArbiters;
  }

  @JsonIgnore
  public Set<InstanceId> getHeartBeatMembers() {
    Set<InstanceId> heartBeatMembers = new HashSet<>();
    heartBeatMembers.addAll(secondaries);
    heartBeatMembers.addAll(arbiters);
    heartBeatMembers.addAll(joiningSecondaries);
    if (secondaryCandidate != null) {
      heartBeatMembers.add(secondaryCandidate);
    }
    return heartBeatMembers;
  }

  @JsonIgnore
  public Set<InstanceId> getAliveSecondaries() {
    Set<InstanceId> activeSecondaries = new HashSet<>();
    activeSecondaries.addAll(secondaries);
    activeSecondaries.addAll(arbiters);
    activeSecondaries.addAll(joiningSecondaries);
    return activeSecondaries;
  }

  @JsonIgnore
  public Set<InstanceId> getWriteSecondaries() {
    Set<InstanceId> writeSecondaries = new HashSet<>();
    writeSecondaries.addAll(secondaries);
    writeSecondaries.addAll(joiningSecondaries);
    return writeSecondaries;
  }

  @JsonIgnore
  public Set<InstanceId> getMembers() {
    Set<InstanceId> members = new HashSet<>();

    members.add(primary);
    members.addAll(secondaries);
    members.addAll(arbiters);
    members.addAll(inactiveSecondaries);
    members.addAll(joiningSecondaries);
    return members;
  }

  @JsonIgnore
  public Set<InstanceId> getPeerInstanceIds(InstanceId myInstanceId) {
    Set<InstanceId> peerIdList = getMembers();
    peerIdList.remove(myInstanceId);
    return peerIdList;
  }

  /**
   * we need to change this function because the meaning of secondaries have been changed.
   */

  public int compareVersion(SegmentMembership o) {
    if (o == null) {
      throw new IllegalArgumentException("segmentMembership is null");
    } else {
      return segmentVersion.compareTo(o.getSegmentVersion());
    }
  }

  /**
   * Note that this function only add newSecondaries to secondaries set instead of to the joining
   * secondaries set.
   */
  public SegmentMembership addSecondaries(InstanceId... newSecondaries) {
    Set<InstanceId> secondariesInNewMembership = new HashSet<>(
        secondaries.size() + newSecondaries.length);
    secondariesInNewMembership.addAll(secondaries);
    boolean changed = secondariesInNewMembership.addAll(Arrays.asList(newSecondaries));

    return new SegmentMembership(
        changed ? this.segmentVersion.incGeneration() : new SegmentVersion(segmentVersion),
        primary, secondariesInNewMembership, this.arbiters, this.inactiveSecondaries,
        this.joiningSecondaries);
  }

  public SegmentMembership addArbiters(InstanceId... newArbiters) {
    Set<InstanceId> arbitersInNewMembership = new HashSet<>(arbiters.size() + newArbiters.length);
    arbitersInNewMembership.addAll(arbiters);
    boolean changed = arbitersInNewMembership.addAll(Arrays.asList(newArbiters));

    return new SegmentMembership(
        changed ? this.segmentVersion.incGeneration() : new SegmentVersion(segmentVersion),
        primary, this.secondaries, arbitersInNewMembership, this.inactiveSecondaries,
        this.joiningSecondaries);
  }

  /**
   * Replace a secondary with a new secondary.If the specified secondary doesn't exist.
   */
  public SegmentMembership replaceSecondary(InstanceId secondary, InstanceId newSecondary) {
    if (!contain(secondary)) {
      // the secondary doesn't exist in the membership, return null to indicate an error
      return null;
    }
    Set<InstanceId> secondariesInNewMembership = new HashSet<>(secondaries.size());
    secondariesInNewMembership.addAll(secondaries);
    secondariesInNewMembership.add(newSecondary);
    secondariesInNewMembership.remove(secondary);

    return new SegmentMembership(this.segmentVersion.incGeneration(), primary,
        secondariesInNewMembership,
        this.arbiters, this.inactiveSecondaries, this.joiningSecondaries);
  }

  public SegmentMembership removeInactiveSecondaryAndAddArbiter(InstanceId inactiveSecondary,
      InstanceId newArbiter) {
    if (!inactiveSecondaries.contains(inactiveSecondary)) {
      return null;
    }

    Set<InstanceId> inactiveSecondariesInNewMembership = new HashSet<>(inactiveSecondaries);
    Set<InstanceId> arbitersInNewMembership = new HashSet<>(arbiters);

    inactiveSecondariesInNewMembership.remove(inactiveSecondary);
    arbitersInNewMembership.add(newArbiter);

    return new SegmentMembership(this.segmentVersion.incGeneration(), primary, secondaries,
        arbitersInNewMembership,
        inactiveSecondariesInNewMembership, this.joiningSecondaries);
  }

  public SegmentMembership removeInactiveSecondaryAndAddJoiningSecondary(
      InstanceId inactiveSecondary,
      InstanceId newSecondary) {
    if (!inactiveSecondaries.contains(inactiveSecondary)) {
      return null;
    }

    Set<InstanceId> joiningSecondariesInNewMembership = new HashSet<>(joiningSecondaries);
    Set<InstanceId> inactiveSecondariesInNewMembership = new HashSet<>(inactiveSecondaries);

    inactiveSecondariesInNewMembership.remove(inactiveSecondary);
    joiningSecondariesInNewMembership.add(newSecondary);

    return new SegmentMembership(this.segmentVersion.incGeneration(), primary, secondaries,
        arbiters,
        inactiveSecondariesInNewMembership, joiningSecondariesInNewMembership);
  }

  /**
   * Note that this function is used only by unit tests now since only inactive secondaries get
   * removed.
   *
   * <p>Remove the existing secondary from the membership and return a new membership with a bigger
   * membership generation. If the secondary is not in the secondary set, return null;
   *
   * @return the new membership
   */
  public SegmentMembership removeSecondary(InstanceId secondary) {
    Set<InstanceId> secondariesInNewMembership = new HashSet<>(secondaries);
    if (secondariesInNewMembership.remove(secondary)) {
      return new SegmentMembership(this.segmentVersion.incGeneration(), primary,
          secondariesInNewMembership,
          this.arbiters, this.inactiveSecondaries, this.joiningSecondaries);
    } else {
      logger.info("membership doesn't contain the secondary ", toString(), secondary);
      return null;
    }
  }

  /**
   * Remove the existing arbiter from the membership and return a new membership with a bigger
   * membership generation. If the secondary is not in the secondary set, return null;
   *
   * @return the new membership
   */
  public SegmentMembership removeArbiter(InstanceId arbiter) {
    Set<InstanceId> arbitersInNewMembership = new HashSet<>(arbiters);
    if (arbitersInNewMembership.remove(arbiter)) {
      return new SegmentMembership(this.segmentVersion.incGeneration(), primary, secondaries,
          arbitersInNewMembership, this.inactiveSecondaries, this.joiningSecondaries);
    } else {
      logger.info("membership doesn't contain the secondary ", toString(), arbiter);
      return null;
    }
  }

  public SegmentMembership newPrimaryChosen(InstanceId newPrimary) {
    if (primary.equals(newPrimary)) {
      return new SegmentMembership(this.segmentVersion.incEpoch(), primary, null, secondaries,
          arbiters, inactiveSecondaries, joiningSecondaries, null, null);
    } else if (isSecondary(newPrimary)) {
      Set<InstanceId> secondariesInNewMembership = new HashSet<>(secondaries);
      Set<InstanceId> inactiveSecondariesInNewMembership = new HashSet<>(inactiveSecondaries);
      secondariesInNewMembership.remove(newPrimary);
      if (tempPrimary != null) {
        // if temp primary is present, the old primary is actually already inactive.
        //
        // and we have to move him to the inactive secondaries set to prevent him to be
        // elected as primary in next election.
        //
        // consider the following case (if old P becomes S):
        //
        // {P S A} ---P died---> {P S(TP) A} ---Voting---> {S P A}
        //
        // by the time the new membership is generated, the old P(now a S) doesn't have
        // all the data but could possibly become primary in next election(eg. all three of
        // them down and the one who has full data(P in new membership) fails to start up or
        // starts up a little bit later)
        inactiveSecondariesInNewMembership.add(primary);
      } else {
        secondariesInNewMembership.add(primary);
      }
      return new SegmentMembership(this.segmentVersion.incEpoch(), newPrimary, null,
          secondariesInNewMembership, arbiters, inactiveSecondariesInNewMembership,
          joiningSecondaries, null, null);
    } else {
      logger.warn("The given new primary {} is not secondary or primary in membership {}",
          newPrimary, this);
      return null;
    }
  }

  public SegmentMembership arbiterBecomeInactive(InstanceId arbiter) {
    Set<InstanceId> arbitersInNewMembership = new HashSet<>(arbiters);
    Set<InstanceId> inactiveSecondariesInNewMembership = new HashSet<>(inactiveSecondaries);

    if (arbitersInNewMembership.remove(arbiter)) {
      inactiveSecondariesInNewMembership.add(arbiter);
      return new SegmentMembership(this.segmentVersion.incGeneration(), primary, tempPrimary,
          secondaries,
          arbitersInNewMembership, inactiveSecondariesInNewMembership, joiningSecondaries, null,
          null);
    } else {
      return null;
    }
  }

  public SegmentMembership aliveSecondaryBecomeInactive(InstanceId secondary) {
    Set<InstanceId> secondariesInNewMembership = new HashSet<>(secondaries);
    Set<InstanceId> joiningSecondariesInNewMembership = new HashSet<>(joiningSecondaries);

    Set<InstanceId> inactiveSecondariesInNewMembership = new HashSet<>(inactiveSecondaries);

    if (secondariesInNewMembership.remove(secondary) || joiningSecondariesInNewMembership
        .remove(secondary)) {
      inactiveSecondariesInNewMembership.add(secondary);
      return new SegmentMembership(this.segmentVersion.incGeneration(), primary, tempPrimary,
          secondariesInNewMembership,
          arbiters, inactiveSecondariesInNewMembership, joiningSecondariesInNewMembership, null,
          null);
    } else {
      return this;
    }
  }

  public SegmentMembership removeInactiveSecondary(InstanceId inactiveSecondary) {
    Set<InstanceId> inactiveSecondariesInNewMembership = new HashSet<>(inactiveSecondaries);

    if (inactiveSecondariesInNewMembership.remove(inactiveSecondary)) {
      SegmentMembership newMembership = new SegmentMembership(this.segmentVersion.incGeneration(),
          primary,
          secondaries, arbiters, inactiveSecondariesInNewMembership, joiningSecondaries);
      newMembership.setQuorumUpdated(quorumUpdated);
      return newMembership;
    } else {
      return this;
    }
  }

  public SegmentMembership addJoiningSecondary(InstanceId joiningSecondary) {
    Set<InstanceId> joiningSecondariesInNewMembership = new HashSet<>(joiningSecondaries);
    if (joiningSecondariesInNewMembership.add(joiningSecondary)) {
      // the joining secondary doesn't exist in the set of the joining secondary at the current
      // membership
      Validate.isTrue(!this.contain(joiningSecondary));
      return new SegmentMembership(this.segmentVersion.incGeneration(), primary, secondaries,
          arbiters,
          inactiveSecondaries, joiningSecondariesInNewMembership);
    } else {
      return this;
    }
  }

  public SegmentMembership joiningSecondaryBecomeSecondary(InstanceId joiningSecondary) {
    Set<InstanceId> joiningSecondariesInNewMembership = new HashSet<>(joiningSecondaries);
    Set<InstanceId> secondariesInNewMembership = new HashSet<>(secondaries);

    if (joiningSecondariesInNewMembership.remove(joiningSecondary)) {
      secondariesInNewMembership.add(joiningSecondary);
      return new SegmentMembership(this.segmentVersion.incGeneration(), primary,
          secondariesInNewMembership,
          arbiters, inactiveSecondaries, joiningSecondariesInNewMembership);
    } else {
      return this;
    }
  }

  public SegmentMembership joiningSecondaryBecomeInactive(InstanceId joiningSecondary) {
    Set<InstanceId> joiningSecondariesInNewMembership = new HashSet<>(joiningSecondaries);
    Set<InstanceId> inactiveSecondariesInNewMembership = new HashSet<>(inactiveSecondaries);

    if (joiningSecondariesInNewMembership.remove(joiningSecondary)) {
      inactiveSecondariesInNewMembership.add(joiningSecondary);
      return new SegmentMembership(this.segmentVersion.incGeneration(), primary, secondaries,
          arbiters,
          inactiveSecondariesInNewMembership, joiningSecondariesInNewMembership);
    } else {
      return this;
    }
  }

  public SegmentMembership inactiveSecondaryBecomeJoining(InstanceId newMember) {
    Set<InstanceId> joiningSecondariesInNewMembership = new HashSet<>(joiningSecondaries);
    Set<InstanceId> inactiveSecondariesInNewMembership = new HashSet<>(inactiveSecondaries);

    if (inactiveSecondariesInNewMembership.remove(newMember)) {
      joiningSecondariesInNewMembership.add(newMember);
      return new SegmentMembership(this.segmentVersion.incGeneration(), primary, secondaries,
          arbiters,
          inactiveSecondariesInNewMembership, joiningSecondariesInNewMembership);
    } else {
      return this;
    }
  }

  public SegmentMembership inactiveSecondaryBecomeArbiter(InstanceId newMember) {
    Set<InstanceId> arbitersInNewMembership = new HashSet<>(arbiters);
    Set<InstanceId> inactiveSecondariesInNewMembership = new HashSet<>(inactiveSecondaries);

    if (inactiveSecondariesInNewMembership.remove(newMember)) {
      arbitersInNewMembership.add(newMember);
      return new SegmentMembership(this.segmentVersion.incGeneration(), primary, secondaries,
          arbitersInNewMembership, inactiveSecondariesInNewMembership, joiningSecondaries);
    } else {
      return this;
    }
  }

  public SegmentMembership secondaryBecomePrimaryCandidate(InstanceId secondary) {
    InstanceId newPrimaryCandidate;
    if (secondaries.contains(secondary) && primaryCandidate == null) {
      newPrimaryCandidate = secondary;
      SegmentVersion newSegmentVersion = this.segmentVersion;
      newSegmentVersion = newSegmentVersion.incGeneration();
      return new SegmentMembership(newSegmentVersion, primary, null, secondaries, arbiters,
          inactiveSecondaries,
          joiningSecondaries, null, newPrimaryCandidate);
    } else {
      return null;
    }
  }

  public SegmentMembership primaryCandidateBecomePrimary(InstanceId primaryCandidate) {
    if (primaryCandidate != null && secondaries.contains(primaryCandidate) && primaryCandidate
        .equals(this.primaryCandidate)) {
      Set<InstanceId> newSecondaries = new HashSet<>(secondaries);
      newSecondaries.remove(primaryCandidate);
      newSecondaries.add(primary);
      return new SegmentMembership(this.segmentVersion.incEpoch(), primaryCandidate, newSecondaries,
          arbiters,
          inactiveSecondaries, joiningSecondaries);
    } else {
      return null;
    }
  }

  public SegmentMembership secondaryBecomeTempPrimary(InstanceId secondary) {
    int incGeneration = getAllSecondaries().size();
    return secondaryBecomeTempPrimary(incGeneration, secondary);
  }

  public SegmentMembership secondaryBecomeTempPrimary(int incGeneration, InstanceId secondary) {
    InstanceId newTempPrimary;
    if (secondaries.contains(secondary)) {
      newTempPrimary = secondary;
      //add more generation to be larger than the old p's generation
      SegmentVersion newSegmentVersion = this.segmentVersion;
      for (int i = 0; i < incGeneration; i++) {
        newSegmentVersion = newSegmentVersion.incGeneration();
      }
      return new SegmentMembership(newSegmentVersion, primary, newTempPrimary, secondaries,
          arbiters,
          inactiveSecondaries, joiningSecondaries, null, null);
    } else {
      return null;
    }
  }

  public SegmentMembership potentialPrimarySelected(InstanceId potentialPrimary) {
    InstanceId newTempPrimary;
    SegmentVersion newSegmentVersion = this.segmentVersion;
    //make sure that inc gen is larger than tp do
    for (int i = 0; i < getAllSecondaries().size() + 1; i++) {
      newSegmentVersion = newSegmentVersion.incGeneration();
    }
    newTempPrimary = tempPrimary == potentialPrimary ? tempPrimary : null;
    return new SegmentMembership(newSegmentVersion, primary, newTempPrimary, secondaries, arbiters,
        inactiveSecondaries, joiningSecondaries, null, null);
  }

  public SegmentMembership addSecondaryCandidate(InstanceId secondaryCandidate) {
    if (secondaryCandidate == null) {
      return this;
    } else {
      return new SegmentMembership(this.segmentVersion.incGeneration(), primary, null, secondaries,
          arbiters,
          inactiveSecondaries, joiningSecondaries, new InstanceId(secondaryCandidate.getId()),
          null);
    }
  }

  public SegmentMembership removeSecondaryCandidate(InstanceId secondaryCandidate) {
    if (this.secondaryCandidate != null && this.secondaryCandidate.equals(secondaryCandidate)) {
      return new SegmentMembership(this.segmentVersion.incGeneration(), primary, null, secondaries,
          arbiters,
          inactiveSecondaries, joiningSecondaries, null, null);
    } else {
      return this;
    }
  }

  public SegmentMembership secondaryCandidateBecomesJoining(InstanceId secondaryCandidate) {
    if (this.secondaryCandidate != null && this.secondaryCandidate.equals(secondaryCandidate)) {
      Set<InstanceId> joiningSecondariesInNewMembership = new HashSet<>(joiningSecondaries);
      joiningSecondariesInNewMembership.add(new InstanceId(secondaryCandidate));
      return new SegmentMembership(this.segmentVersion.incGeneration(), primary, null, secondaries,
          arbiters,
          inactiveSecondaries, joiningSecondariesInNewMembership, null, null);
    } else {
      return this;
    }
  }

  public SegmentMembership secondaryCandidateBecomesSecondaryAndRemoveTheReplacee(
      InstanceId secondaryCandidate,
      InstanceId replacee) {
    if (this.secondaryCandidate != null && this.secondaryCandidate.equals(secondaryCandidate)) {
      Set<InstanceId> secondariesInNewMembership = new HashSet<>(secondaries);
      Set<InstanceId> inactiveSecondariesInNewMembership = new HashSet<>(inactiveSecondaries);
      Set<InstanceId> joiningSecondariesInNewMembership = new HashSet<>(joiningSecondaries);
      if (secondariesInNewMembership.remove(replacee) || inactiveSecondariesInNewMembership
          .remove(replacee)
          || joiningSecondariesInNewMembership.remove(replacee)) {
        secondariesInNewMembership.add(new InstanceId(secondaryCandidate));
        return new SegmentMembership(this.segmentVersion.incGeneration(), primary, null,
            secondariesInNewMembership, arbiters, inactiveSecondariesInNewMembership,
            joiningSecondariesInNewMembership, null, null);
      } else {
        return this;
      }
    } else {
      return this;
    }
  }

  public boolean isQuorumUpdated() {
    return quorumUpdated;
  }

  public void setQuorumUpdated(boolean quorumUpdated) {
    this.quorumUpdated = quorumUpdated;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof SegmentMembership)) {
      return false;
    }

    SegmentMembership that = (SegmentMembership) o;

    if (segmentVersion != null ? !segmentVersion.equals(that.segmentVersion)
        : that.segmentVersion != null) {
      return false;
    }
    if (!primary.equals(that.primary)) {
      return false;
    }
    if (tempPrimary != null ? !tempPrimary.equals(that.tempPrimary) : that.tempPrimary != null) {
      return false;
    }
    if (secondaries != null ? !secondaries.equals(that.secondaries) : that.secondaries != null) {
      return false;
    }
    if (arbiters != null ? !arbiters.equals(that.arbiters) : that.arbiters != null) {
      return false;
    }
    if (inactiveSecondaries != null
        ? !inactiveSecondaries.equals(that.inactiveSecondaries)
        : that.inactiveSecondaries != null) {
      return false;
    }
    if (joiningSecondaries != null
        ? !joiningSecondaries.equals(that.joiningSecondaries) : that.joiningSecondaries != null) {
      return false;
    }
    if (secondaryCandidate != null
        ? !secondaryCandidate.equals(that.secondaryCandidate) : that.secondaryCandidate != null) {
      return false;
    }
    if (primaryCandidate != null
        ? !primaryCandidate.equals(that.primaryCandidate) : that.primaryCandidate != null) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = segmentVersion != null ? segmentVersion.hashCode() : 0;
    result = prime * result + primary.hashCode();
    result = prime * result + (tempPrimary != null ? tempPrimary.hashCode() : 0);
    result = prime * result + (secondaries != null ? secondaries.hashCode() : 0);
    result = prime * result + (arbiters != null ? arbiters.hashCode() : 0);
    result = prime * result + (inactiveSecondaries != null ? inactiveSecondaries.hashCode() : 0);
    result = prime * result + (joiningSecondaries != null ? joiningSecondaries.hashCode() : 0);
    result = prime * result + (secondaryCandidate != null ? secondaryCandidate.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder("SegmentMembership {");

    builder.append(segmentVersion);
    builder.append(", primary=").append(primary).append(", tempPrimary=").append(tempPrimary);

    if (!secondaries.isEmpty()) {
      builder.append(", secondaries=").append(secondaries);
    }

    if (!arbiters.isEmpty()) {
      builder.append(", arbiters=").append(arbiters);
    }

    if (!inactiveSecondaries.isEmpty()) {
      builder.append(", inactiveSecondary=").append(inactiveSecondaries);
    }

    if (!joiningSecondaries.isEmpty()) {
      builder.append(", joiningSecondary=").append(joiningSecondaries);
    }

    if (secondaryCandidate != null) {
      builder.append(", secondaryCandidate=").append(secondaryCandidate);
    }

    if (primaryCandidate != null) {
      builder.append(", primaryCandidate=").append(primaryCandidate);
    }

    builder.append(", quorumUpdated=").append(quorumUpdated);

    builder.append("}");
    return builder.toString();
  }

  public int compareEpoch(SegmentMembership other) {
    int equal = 0;
    int iamOlder = 1;
    if (other == null) {
      return iamOlder;
    }

    if (this.equals(other)) {
      return equal;
    } else {
      return this.segmentVersion.getEpoch() - other.getSegmentVersion().getEpoch();
    }
  }

  public int compareGeneration(SegmentMembership other) {
    int equal = 0;
    int iamOlder = 1;
    if (other == null) {
      return iamOlder;
    }

    if (this.equals(other)) {
      return equal;
    } else {
      return this.segmentVersion.getGeneration() - other.getSegmentVersion().getGeneration();
    }
  }

  public boolean hasSameEpochLowerGeneration(SegmentMembership other) {
    return (segmentVersion.getEpoch() == other.getSegmentVersion().getEpoch()
        && segmentVersion.getGeneration() < other.getSegmentVersion().getGeneration());
  }

  public boolean hasSameEpochHigherGeneration(SegmentMembership other) {
    return (segmentVersion.getEpoch() == other.getSegmentVersion().getEpoch()
        && segmentVersion.getGeneration() > other.getSegmentVersion().getGeneration());
  }

  public int compareTo(SegmentMembership other) throws InvalidMembershipException {
    int iamYounger = -1;
    int equal = 0;
    int iamOlder = 1;
    if (other == null) {
      return iamOlder;
    }

    if (this.equals(other)) {
      return equal;
    } else {
      int compared = this.compareVersion(other);
      if (compared > 0) {
        return iamOlder;
      } else if (compared == 0) {
        throw new InvalidMembershipException(
            "The existing membership " + this + " has the same version with " + other
                + " but they have different members");
      } else {
        return iamYounger;
      }
    }
  }

  public int size() {
    return 1 + secondaries.size() + arbiters.size() + inactiveSecondaries.size()
        + joiningSecondaries.size();
  }

  public int aliveSize() {
    return 1 + secondaries.size() + arbiters.size() + joiningSecondaries.size();
  }

  /**
   * Decide whether this membership allows a segment unit joins in to become a joining secondary.
   * The following conditions have to be met at the same time.
   *
   * <p>1. there are some missing secondaries in the membership 2. no joining secondaries at the
   * membership yet
   */
  public boolean allowNewJoiningSecondary(int ntotalSecondaries) {
    return secondaries.size() < ntotalSecondaries && joiningSecondaries.size() == 0;
  }

  /**
   * after broadcast to neccessory members, some alreay response bad result we can figure out the
   * possibility of success by supposing all the left members may response good and call the
   * checkGood method. if the checkGood ret is false, means no possibility, this method return true
   * to tell the P fail.
   */
  public boolean checkBadWriteResultOfSecondariesAndArbiters(int nwriteQuorum,
      int badSecondariesCount,
      int badJoiningSecondariesCount, int badArbitersCount) {
    int broadcastSecondariesCount = secondaries.size();
    if (tempPrimary != null) {
      broadcastSecondariesCount--;
    }
    int goodSecondariesCount = broadcastSecondariesCount - badSecondariesCount;
    int goodJoiningSecondariesCount = joiningSecondaries.size() - badJoiningSecondariesCount;
    int goodArbitersCount = arbiters.size() - badArbitersCount;
    return !checkWriteResultOfSecondariesAndArbiters(nwriteQuorum, goodSecondariesCount,
        goodJoiningSecondariesCount, goodArbitersCount);
  }

  /**
   * <p>
   * Write requests(including writes from C and broadcasts from P or TP) should be processed
   * successfully on subQuorum(nwriteQuorum - 1) of members. Those members could be Secondaries,
   * Joining Secondaries, or Arbiters. But we have some extra principles :
   * </p>
   *
   * <p>
   * 1. If number of S is greater than subQuorum, results of J and A are not considered.<br> 2. If
   * number of S and J is greater than subQuorum, results of A are not considered.<br>
   * </p>
   *
   * @param nwriteQuorum                num of write quorum
   * @param goodSecondariesCount        num of good secondaries
   * @param goodJoiningSecondariesCount num of good joining secondaries
   * @param goodArbitersCount           num of good arbiters
   * @return true if it's already a successful write.
   */
  public boolean checkWriteResultOfSecondariesAndArbiters(int nwriteQuorum,
      int goodSecondariesCount,
      int goodJoiningSecondariesCount, int goodArbitersCount) {
    //def q = subQuorum
    int subQuorum = nwriteQuorum - 1;
    //def s = numS or numS - 1 (if tp exist)
    int broadcastSecondariesCount = secondaries.size();
    if (tempPrimary != null) {
      broadcastSecondariesCount--;
    }

    // ensure gs >= min(q,s)
    if (goodSecondariesCount < Math.min(subQuorum, broadcastSecondariesCount)) {
      return false;
    }

    // if gs >= q, can return
    if (goodSecondariesCount >= subQuorum) {
      return true;
    }

    //else ensure gj >= min(q-gs,j)
    if (goodJoiningSecondariesCount < Math
        .min(subQuorum - goodSecondariesCount, joiningSecondaries.size())) {
      return false;
    }

    //if gs + gj >= q, can return
    if (goodSecondariesCount + goodJoiningSecondariesCount >= subQuorum) {
      return true;
    }

    //else ensure gs + gj + ga >= q
    return goodSecondariesCount + goodJoiningSecondariesCount + goodArbitersCount >= subQuorum;
  }

  // check if all secondaries required by the specified volume present
  public boolean allSecondariesPresent(VolumeType volumeType) {
    return secondaries.size() == volumeType.getNumSecondaries();
  }

  // TODO : remove this

  public void markMemberIoStatus(InstanceId member, MemberIoStatus memberIoStatus) {
    if (memberIoStatus == null) {
      logger.warn("won't don anything if want set:{} as null status at:{}", member, this);
      return;
    }
    // check if in this membership
    if (!contain(member)) {
      Validate.isTrue(false,
          "In membership:" + this + ", want to mark:" + member + ", status:" + memberIoStatus);
    }

    MemberIoStatus currentMemberIoStatus = memberIoStatusMap.get(member);
    if (currentMemberIoStatus == null) {
      Validate
          .isTrue(false, "member:" + member + " must be taken out before in membership:" + this);
    }

    // mark one member status should follow GOOD->BAD(end)
    synchronized (memberIoStatusMap) {
      if (currentMemberIoStatus.canMoveTo(memberIoStatus)) {
        memberIoStatusMap.put(member, memberIoStatus);
      } else {
        logger.warn("In current membership, this:{} @status:{} can not turn to other status:{}",
            member,
            currentMemberIoStatus, memberIoStatus);
      }
    }
  }

  /**
   * if member doesn't exist in map record, means it needs to be init.
   */
  // TODO : remove this
  public MemberIoStatus getMemberIoStatus(InstanceId instanceId) {
    MemberIoStatus memberIoStatus = memberIoStatusMap.get(instanceId);
    if (memberIoStatus == null) {
      // should init member io status
      if (isPrimary(instanceId)) {
        memberIoStatus = MemberIoStatus.Primary;
      } else if (isTempPrimary(instanceId)) {
        memberIoStatus = MemberIoStatus.TempPrimary;
      } else if (isSecondary(instanceId)) {
        memberIoStatus = MemberIoStatus.Secondary;
      } else if (isJoiningSecondary(instanceId)) {
        memberIoStatus = MemberIoStatus.JoiningSecondary;
      } else if (isArbiter(instanceId)) {
        memberIoStatus = MemberIoStatus.Arbiter;
      } else if (isInactiveSecondary(instanceId)) {
        // TODO : why this is warn ?
        logger.warn("please watch, you get an inactive member {}", instanceId);
        memberIoStatus = MemberIoStatus.InactiveSecondary;
      } else {
        Validate.isTrue(false,
            "it is not happen to get:" + instanceId + ", because it's not in current membership:"
                + this);
        memberIoStatus = MemberIoStatus.ExternalMember;
      }
      memberIoStatusMap.put(instanceId, memberIoStatus);
    }
    return memberIoStatus;
  }

  /**
   * when two membership are same, should merge their status, follow good ==> down rule.
   */
  // TODO remove this
  public void mergeMemberStatus(/*just for logger*/Long requestId,
      SegmentMembership otherMembership) {
    if (otherMembership == null) {
      return;
    }
    Validate.isTrue(this.compareTo(otherMembership) == 0);
    // process two io member status recorder
    for (InstanceId otherMember : otherMembership.getMembers()) {
      // just ignore inactive secondary
      if (otherMembership.isInactiveSecondary(otherMember)) {
        continue;
      }
      MemberIoStatus otherMemberIoStatus = otherMembership.getMemberIoStatus(otherMember);
      MemberIoStatus currentMemberIoStatus = this.getMemberIoStatus(otherMember);
      MemberIoStatus mergeMemberIoStatus = currentMemberIoStatus
          .mergeMemberIoStatus(otherMemberIoStatus);
      if (currentMemberIoStatus != mergeMemberIoStatus) {
        logger.warn(
            "ori:{} current membership:{} mark member:{} status:{}, use current MemberIOStatus:{}, "
                + "other MemberIOStatus:{}",
            requestId, this, otherMember, mergeMemberIoStatus, currentMemberIoStatus,
            otherMemberIoStatus);
        markMemberIoStatus(otherMember, mergeMemberIoStatus);
      }
    }
  }

  /**
   * just for coordinator logger for now.
   */
  public Map<InstanceId, MemberIoStatus> getMemberIoStatusMap() {
    return memberIoStatusMap;
  }

  public String serializeToObjectMapperContent() {
    String value = "";
    value += segmentVersion.getEpoch();
    value += ",";
    value += segmentVersion.getGeneration();
    value += ",";
    value += primary == null ? null_value : primary.getId();
    value += ",";
    value += tempPrimary == null ? null_value : tempPrimary.getId();
    value += ",";
    Iterator<InstanceId> idIterator = secondaries.iterator();
    while (idIterator.hasNext()) {
      InstanceId instanceId = idIterator.next();
      value += instanceId.getId();
      if (!idIterator.hasNext()) {
        break;
      }
      value += ":";
    }
    value += ",";
    idIterator = arbiters.iterator();
    while (idIterator.hasNext()) {
      InstanceId instanceId = idIterator.next();
      value += instanceId.getId();
      if (!idIterator.hasNext()) {
        break;
      }
      value += ":";
    }
    value += ",";
    idIterator = inactiveSecondaries.iterator();
    while (idIterator.hasNext()) {
      InstanceId instanceId = idIterator.next();
      value += instanceId.getId();
      if (!idIterator.hasNext()) {
        break;
      }
      value += ":";
    }
    value += ",";
    idIterator = joiningSecondaries.iterator();
    while (idIterator.hasNext()) {
      InstanceId instanceId = idIterator.next();
      value += instanceId.getId();
      if (!idIterator.hasNext()) {
        break;
      }
      value += ":";
    }
    value += ",";
    value += primaryCandidate == null ? null_value : primaryCandidate.getId();
    value += ",";
    value += secondaryCandidate == null ? null_value : secondaryCandidate.getId();
    logger.debug("segment member ship to json value is {}", value);
    return value;
  }
}