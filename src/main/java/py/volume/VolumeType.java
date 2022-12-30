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

package py.volume;

import java.security.InvalidParameterException;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.Validate;
import py.membership.SegmentForm;
import py.thrift.share.VolumeTypeThrift;

/**
 * this enumeration defines the volume type supported by the data node.
 */
public enum VolumeType {
  // 3 members per segment, 2 secondaries and both the writing quorum size and voting quorum are 2
  REGULAR(3, 2, 2, 2) {
    public VolumeTypeThrift getVolumeTypeThrift() {
      return VolumeTypeThrift.REGULAR;
    }

    @Override
    public SegmentForm getSegmentForm(int secondariesCount, int joiningSecondaryCount,
        int arbiterCount, int inactiveSecondariesCount) {
      Validate.isTrue(arbiterCount == 0);
      Validate.isTrue(secondariesCount + joiningSecondaryCount <= getNumMembers() - 1);
      int needInactiveSecondariesCount =
          getNumMembers() - secondariesCount - joiningSecondaryCount - 1;
      needInactiveSecondariesCount = Math
          .min(inactiveSecondariesCount, needInactiveSecondariesCount);

      //the 2 201 3000 40000 just for get hash number,if change must change value for SegmentForm
      int tempKey = secondariesCount * HASH_FOR_COUNT + joiningSecondaryCount * HASH_FOR_COUNT_ONE
          + needInactiveSecondariesCount * HASH_FOR_COUNT_THREE;
      return mapForPss.get(tempKey);
    }

  },

  // 3 members per segment, 1 secondary and 1 arbiter and both the writing quorum size and voting
  // quorum are 2
  SMALL(3, 1, 2, 2) {
    public VolumeTypeThrift getVolumeTypeThrift() {
      return VolumeTypeThrift.SMALL;
    }

    @Override
    public SegmentForm getSegmentForm(int secondariesCount, int joiningSecondaryCount,
        int arbiterCount, int inactiveSecondariesCount) {
      Validate
          .isTrue(secondariesCount + joiningSecondaryCount + arbiterCount <= getNumMembers() - 1);
      int needInactiveSecondariesCount =
          getNumMembers() - secondariesCount - joiningSecondaryCount - arbiterCount - 1;
      needInactiveSecondariesCount = Math
          .min(inactiveSecondariesCount, needInactiveSecondariesCount);
      //the 2 201 3000 40000 just for get hash number,if change must change value for SegmentForm
      int tempSmallKey =
          secondariesCount * HASH_FOR_COUNT + joiningSecondaryCount * HASH_FOR_COUNT_TWO
              + needInactiveSecondariesCount * HASH_FOR_COUNT_THREE
              + arbiterCount * HASH_FOR_COUNT_FOUR;

      return mapForPsa.get(tempSmallKey);

    }

  },

  // 5 members per segment, 2 secondaries and 2 arbiters and 3 is the quorum size
  LARGE(5, 2, 3, 3) {
    public VolumeTypeThrift getVolumeTypeThrift() {
      return VolumeTypeThrift.LARGE;
    }

    @Override
    public SegmentForm getSegmentForm(int secondariesCount, int joiningSecondaryCount,
        int arbiterCount, int inactiveSecondariesCount) {
      Validate
          .isTrue(secondariesCount + joiningSecondaryCount + arbiterCount <= getNumMembers() - 1);
      int needInactiveSecondariesCount =
          getNumMembers() - secondariesCount - joiningSecondaryCount - arbiterCount - 1;
      needInactiveSecondariesCount = Math
          .min(inactiveSecondariesCount, needInactiveSecondariesCount);
      //the 2 201 3000 40000 just for get hash number,if change must change value for SegmentForm
      int tempKey = secondariesCount * HASH_FOR_COUNT + arbiterCount * HASH_FOR_COUNT_ONE
          + joiningSecondaryCount * HASH_FOR_COUNT_THREE
          + needInactiveSecondariesCount * HASH_FOR_COUNT_FOUR;
      return mapForPssaa.get(tempKey);
    }
  };

  public static final VolumeType DEFAULT_VOLUME_TYPE = REGULAR;
  public static final int HASH_FOR_COUNT = 2;
  public static final int HASH_FOR_COUNT_ONE = 200;
  public static final int HASH_FOR_COUNT_TWO = 201;
  public static final int HASH_FOR_COUNT_THREE = 3000;
  public static final int HASH_FOR_COUNT_FOUR = 40000;
  public final Map<Integer, SegmentForm> mapForPsa = new HashMap<Integer, SegmentForm>() {
    {
      put(40000, SegmentForm.PA);
      put(3000, SegmentForm.TPI); //PI
      put(43000, SegmentForm.PIA);
      put(201, SegmentForm.TPJ); //PJ
      put(40201, SegmentForm.PJA);
      put(3201, SegmentForm.TPJ); //PJI
      put(2, SegmentForm.TPS); //PS
      put(40002, SegmentForm.PSA);
      put(3002, SegmentForm.TPS); //PSI
      put(6000, SegmentForm.TPI); //PII
    }
  };
  public final Map<Integer, SegmentForm> mapForPss = new HashMap<Integer, SegmentForm>() {
    {
      put(3000, SegmentForm.PI);
      put(6000, SegmentForm.PII);
      put(200, SegmentForm.PJ);
      put(3200, SegmentForm.PJI);
      put(400, SegmentForm.PJJ);
      put(2, SegmentForm.PS);
      put(3002, SegmentForm.PSI);
      put(202, SegmentForm.PSJ);
      put(4, SegmentForm.PSS);
    }
  };
  public final Map<Integer, SegmentForm> mapForPssaa = new HashMap<Integer, SegmentForm>() {
    {
      put(160000, SegmentForm.PIIII);
      put(123000, SegmentForm.PJIII);
      put(86000, SegmentForm.PJJII);
      put(120200, SegmentForm.PIIAI);
      put(83200, SegmentForm.PJIAI);
      put(46200, SegmentForm.PJJAI);
      put(80400, SegmentForm.PIIAA);
      put(43400, SegmentForm.PJIAA);
      put(6400, SegmentForm.PJJAA);
      put(120002, SegmentForm.PSIII);
      put(83002, SegmentForm.PSJII);
      put(80202, SegmentForm.PSIAI);
      put(43202, SegmentForm.PSJAI);
      put(40402, SegmentForm.PSIAA);
      put(3402, SegmentForm.PSJAA);
      put(80004, SegmentForm.PSSII);
      put(40204, SegmentForm.PSSAI);
      put(404, SegmentForm.PSSAA);

      put(120000, SegmentForm.PIII);
      put(83000, SegmentForm.PJII);
      put(80200, SegmentForm.PIAI);
      put(43200, SegmentForm.PJAI);
      put(40400, SegmentForm.PIAA);
      put(3400, SegmentForm.PJAA);
      put(80002, SegmentForm.PSII);
      put(40202, SegmentForm.PSAI);
      put(402, SegmentForm.PSAA);

      put(400, SegmentForm.PAA);
    }
  };
  private final int numMembers;
  private final int numSecondaries;
  private final int numArbiters;
  private final int writeQuorumSize;
  private final int votingQuorumSize;

  VolumeType(int numMembers, int numSecondaries, int writeQuorumSize, int votingQuorumSize) {
    this.numMembers = numMembers;
    this.numSecondaries = numSecondaries;
    // take the primary out
    this.numArbiters = numMembers - numSecondaries - 1;
    this.writeQuorumSize = writeQuorumSize;
    this.votingQuorumSize = votingQuorumSize;
    if (numArbiters < 0 || writeQuorumSize < votingQuorumSize) {
      throw new InvalidParameterException(
          "number of secondaries " + numSecondaries + " is larger than the number of members "
              + numMembers
              + " or the write quorum " + writeQuorumSize + "is less than voting quorum size "
              + votingQuorumSize);
    }
  }

  public int getNumMembers() {
    return numMembers;
  }

  public int getNumSecondaries() {
    return numSecondaries;
  }

  public int getNumArbiters() {
    return numArbiters;
  }

  public int getWriteQuorumSize() {
    return writeQuorumSize;
  }

  public int getVotingQuorumSize() {
    return votingQuorumSize;
  }

  public VolumeTypeThrift getVolumeTypeThrift() {
    throw new NotImplementedException(
        "not support the type=" + numArbiters + "," + numSecondaries + "," + numArbiters + ","
            + writeQuorumSize
            + "," + votingQuorumSize);
  }

  public SegmentForm getSegmentForm(int secondariesCount, int joiningSecondaryCount,
      int arbiterCount, int inactiveSecondariesCount) {
    SegmentForm segmentForm = null;
    return segmentForm;

  }

}

