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

import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.instance.InstanceId;
import py.volume.VolumeType;

/**
 * 'P' == Primary, 'S' == Secondary, 'J' == JoiningSecondary, 'I' == InactiveSecondary 'A' ==
 * Arbiter. 'T'== Two-copies form
 */

//TODO: do we have more than quorum  I in membership, like PII PIIAI?
public enum SegmentForm {
  PSS(1) {
    @Override
    public boolean mergeCreateLogResult(int goodPrimaryCount, int goodSecondariesCount,
        int goodJoiningSecondariesCount, IoActionContext ioActionContext, VolumeType volumeType) {
      boolean createSuccess = false;
      // if primary down, should sent to SS
      if (ioActionContext.isPrimaryDown()) {
        if (goodSecondariesCount >= volumeType.getWriteQuorumSize()) {
          createSuccess = true;
        }
      } else {
        // send PS or PSS
        if (goodPrimaryCount > 0 && goodSecondariesCount > 0) {
          createSuccess = true;
        }
      }
      return createSuccess;
    }

    @Override
    public boolean mergeCommitLogResult(int goodPrimaryCount, int goodSecondariesCount,
        int goodJoiningSecondariesCount, IoActionContext ioActionContext, VolumeType volumeType) {
      boolean canCommit = false;
      // if primary down, should sent to SS
      if (ioActionContext.isPrimaryDown()) {
        if (goodSecondariesCount >= volumeType.getWriteQuorumSize()) {
          canCommit = true;
        }
      } else {
        // send PS or PSS
        if (goodPrimaryCount > 0 && goodSecondariesCount > 0) {
          canCommit = true;
        }
      }
      return canCommit;
    }

    @Override
    public boolean mergeReadLogResult(int goodPrimaryCount,
        SecondariesCountInfo secondariesCountInfo,
        int goodJoiningSecondariesCount, int goodArbiterCount, IoActionContext ioActionContext,
        VolumeType volumeType) {
      boolean readSuccess = false;
      int goodSecondariesCount = secondariesCountInfo.allSecondariesCount();
      // if primary down, should sent to SS
      if (ioActionContext.isPrimaryDown()) {
        if (ioActionContext.isMetReadDownSecondary()) {
          if (goodSecondariesCount > 0) {
            readSuccess = true;
          }
        } else if (secondariesCountInfo.getFetchCount() > 0
            && goodSecondariesCount >= volumeType.getWriteQuorumSize()) {
          readSuccess = true;
        }
      } else {
        // send PS or PSS
        if (goodPrimaryCount > 0 && goodSecondariesCount > 0) {
          readSuccess = true;
        }
      }
      return readSuccess;
    }

    @Override
    public void processWriteIoActionContext(int primaryCount, int secondariesCount,
        int joiningSecondariesCount,
        IoActionContext ioActionContext, VolumeType volumeType) {
      // determine write could success or not
      if (primaryCount == 0) {
        // can not write any more
        if (secondariesCount < volumeType.getWriteQuorumSize()) {
          ioActionContext.setResendDirectly(true);
        }
      } else if (primaryCount > 0) {
        if (secondariesCount == 0) {
          ioActionContext.setResendDirectly(true);
        }
      } else {
        Validate.isTrue(false);
      }
    }

    @Override
    public void processReadIoActionContext(int primaryCount, int secondariesCount,
        int joiningSecondariesCount,
        int goodArbiterCount, IoActionContext ioActionContext, VolumeType volumeType) {
      // determine read could success or not
      if (primaryCount == 0) {
        // can not read any more
        if (ioActionContext.isMetReadDownSecondary()) {
          if (secondariesCount == 0) {
            ioActionContext.setResendDirectly(true);
          }
        } else if (secondariesCount < volumeType.getWriteQuorumSize()) {
          ioActionContext.setResendDirectly(true);
        }
      } else if (primaryCount > 0) {
        if (secondariesCount == 0) {
          ioActionContext.setResendDirectly(true);
        }
      } else {
        Validate.isTrue(false);
      }
    }

    @Override
    public boolean writeDoneDirectly(int primaryDisconnectCount, int secondaryDisconnectCount,
        int joiningSecondaryDisconnectCount, int arbiterDisconnectCount, VolumeType volumeType) {
      if (primaryDisconnectCount + secondaryDisconnectCount >= volumeType.getWriteQuorumSize()) {
        return true;
      }
      return false;
    }

    @Override
    public boolean readDoneDirectly(int primaryDisconnectCount, int secondaryDisconnectCount,
        int joiningSecondaryDisconnectCount, int arbiterDisconnectCount, VolumeType volumeType) {
      if (primaryDisconnectCount + secondaryDisconnectCount >= volumeType.getWriteQuorumSize()) {
        return true;
      }
      return false;
    }

    @Override
    public boolean canGenerateNewPrimary() {
      return true;
    }

    @Override
    public boolean isSkipFromSourceVolumeData(int secondarySkipConut) {
      return secondarySkipConut >= 2;
    }
  },

  PSJ(2) {
    @Override
    public boolean mergeCreateLogResult(int goodPrimaryCount, int goodSecondariesCount,
        int goodJoiningSecondariesCount, IoActionContext ioActionContext, VolumeType volumeType) {
      boolean createSuccess = false;
      // if primary down, should sent to SJ
      if (ioActionContext.isPrimaryDown()) {
        if ((goodSecondariesCount + goodJoiningSecondariesCount) >= volumeType
            .getWriteQuorumSize()) {
          createSuccess = true;
        }
      } else {
        // send PS or PSJ
        if (goodPrimaryCount > 0) {
          // sent PJ
          if (ioActionContext.isSecondaryDown()) {
            if (goodJoiningSecondariesCount > 0) {
              createSuccess = true;
            }
          } else {
            // should sent to PS or PSJ
            if (goodSecondariesCount > 0) {
              createSuccess = true;
            }
          }
        }
      }
      return createSuccess;
    }

    @Override
    public boolean mergeReadLogResult(int goodPrimaryCount,
        SecondariesCountInfo secondariesCountInfo,
        int goodJoiningSecondariesCount, int goodArbiterCount, IoActionContext ioActionContext,
        VolumeType volumeType) {
      boolean readSuccess = false;
      int goodSecondariesCount = secondariesCountInfo.allSecondariesCount();
      // if primary down, should sent to SJ
      if (ioActionContext.isPrimaryDown()) {
        if ((goodSecondariesCount + goodJoiningSecondariesCount) >= volumeType
            .getWriteQuorumSize()) {
          readSuccess = true;
        }
      } else {
        // send PS or PSJ
        if (goodPrimaryCount > 0 && (goodSecondariesCount + goodJoiningSecondariesCount) > 0) {
          readSuccess = true;
        }
      }
      return readSuccess;
    }

    @Override
    public boolean mergeCommitLogResult(int goodPrimaryCount, int goodSecondariesCount,
        int goodJoiningSecondariesCount, IoActionContext ioActionContext, VolumeType volumeType) {
      boolean canCommit = false;
      // if primary down, should sent to SJ
      if (ioActionContext.isPrimaryDown()) {
        if ((goodSecondariesCount + goodJoiningSecondariesCount) >= volumeType
            .getWriteQuorumSize()) {
          canCommit = true;
        }
      } else {
        // send PS or PSJ
        if (goodPrimaryCount > 0) {
          // sent PJ
          if (ioActionContext.isSecondaryDown()) {
            if (goodJoiningSecondariesCount > 0) {
              canCommit = true;
            }
          } else {
            // should sent to PS or PSJ
            if (goodSecondariesCount > 0) {
              canCommit = true;
            }
          }
        }
      }
      return canCommit;
    }

    @Override
    public void processWriteIoActionContext(int primaryCount, int secondariesCount,
        int joiningSecondariesCount,
        IoActionContext ioActionContext, VolumeType volumeType) {
      if (primaryCount == 0) {
        if (secondariesCount + joiningSecondariesCount < volumeType.getWriteQuorumSize()) {
          ioActionContext.setResendDirectly(true);
        }
      } else if (primaryCount > 0) {
        if (secondariesCount + joiningSecondariesCount == 0) {
          ioActionContext.setResendDirectly(true);
        }
      } else {
        Validate.isTrue(false);
      }
    }

    @Override
    public void processReadIoActionContext(int primaryCount, int secondariesCount,
        int joiningSecondariesCount,
        int goodArbiterCount, IoActionContext ioActionContext, VolumeType volumeType) {
      if (primaryCount == 0) {
        if (secondariesCount + joiningSecondariesCount < volumeType.getWriteQuorumSize()) {
          ioActionContext.setResendDirectly(true);
        }
      } else if (primaryCount > 0) {
        if (secondariesCount + joiningSecondariesCount == 0) {
          ioActionContext.setResendDirectly(true);
        }
      } else {
        Validate.isTrue(false);
      }
    }

    @Override
    public boolean writeDoneDirectly(int primaryDisconnectCount, int secondaryDisconnectCount,
        int joiningSecondaryDisconnectCount, int arbiterDisconnectCount, VolumeType volumeType) {
      if (primaryDisconnectCount + secondaryDisconnectCount + joiningSecondaryDisconnectCount
          >= volumeType
          .getWriteQuorumSize()) {
        return true;
      }
      return false;
    }

    @Override
    public boolean readDoneDirectly(int primaryDisconnectCount, int secondaryDisconnectCount,
        int joiningSecondaryDisconnectCount, int arbiterDisconnectCount, VolumeType volumeType) {
      if (primaryDisconnectCount + secondaryDisconnectCount + joiningSecondaryDisconnectCount
          >= volumeType
          .getWriteQuorumSize()) {
        return true;
      }
      return false;
    }

    @Override
    public boolean canGenerateNewPrimary() {
      return true;
    }

    @Override
    public boolean isSkipFromSourceVolumeData(int secondarySkipConut) {
      return secondarySkipConut >= 2;
    }
  },

  PSI(3) {
    @Override
    public boolean mergeCreateLogResult(int goodPrimaryCount, int goodSecondariesCount,
        int goodJoiningSecondariesCount, IoActionContext ioActionContext, VolumeType volumeType) {
      // if primary down, stop write
      boolean createSuccess = false;
      if (ioActionContext.isPrimaryDown()) {
        logger.error("if primary down, can not sent to members in this:{}", this);
      } else {
        // send PS
        if (goodPrimaryCount > 0 && goodSecondariesCount > 0) {
          createSuccess = true;
        }
      }
      return createSuccess;
    }

    @Override
    public boolean mergeReadLogResult(int goodPrimaryCount,
        SecondariesCountInfo secondariesCountInfo,
        int goodJoiningSecondariesCount, int goodArbiterCount, IoActionContext ioActionContext,
        VolumeType volumeType) {
      // if primary down(but no way to mark primary down), still can read from S
      boolean readSuccess = false;
      if (!ioActionContext.isPrimaryDown()) {
        if (goodPrimaryCount > 0) {
          readSuccess = true;
        }
      }
      return readSuccess;
    }

    @Override
    public boolean mergeCommitLogResult(int goodPrimaryCount, int goodSecondariesCount,
        int goodJoiningSecondariesCount, IoActionContext ioActionContext, VolumeType volumeType) {
      // if primary down, stop write
      boolean canCommit = false;
      if (ioActionContext.isPrimaryDown()) {
        logger.error("if primary down, can not sent to members in this:{}", this);
      } else {
        // send PS
        if (goodPrimaryCount > 0 && goodSecondariesCount > 0) {
          canCommit = true;
        }
      }
      return canCommit;
    }

    @Override
    public void processWriteIoActionContext(int primaryCount, int secondariesCount,
        int joiningSecondariesCount,
        IoActionContext ioActionContext, VolumeType volumeType) {
      if ((primaryCount + secondariesCount) < volumeType.getWriteQuorumSize()) {
        ioActionContext.setResendDirectly(true);
      }
    }

    @Override
    public void processReadIoActionContext(int primaryCount, int secondariesCount,
        int joiningSecondariesCount,
        int goodArbiterCount, IoActionContext ioActionContext, VolumeType volumeType) {
      // if primary down(but no way to mark primary down), still can read from S
      if (primaryCount == 0) {
        ioActionContext.setResendDirectly(true);
      } else {
        ioActionContext.doNotNeedCheckRead();
      }
    }

    @Override
    public boolean writeDoneDirectly(int primaryDisconnectCount, int secondaryDisconnectCount,
        int joiningSecondaryDisconnectCount, int arbiterDisconnectCount, VolumeType volumeType) {
      if (primaryDisconnectCount + secondaryDisconnectCount > 0) {
        return true;
      }
      return false;
    }

    @Override
    public boolean readDoneDirectly(int primaryDisconnectCount, int secondaryDisconnectCount,
        int joiningSecondaryDisconnectCount, int arbiterDisconnectCount, VolumeType volumeType) {
      if (primaryDisconnectCount > 0) {
        return true;
      }
      return false;
    }

    @Override
    public boolean canGenerateNewPrimary() {
      return true;
    }
  },

  PJI(4) {
    @Override
    public boolean mergeCreateLogResult(int goodPrimaryCount, int goodSecondariesCount,
        int goodJoiningSecondariesCount, IoActionContext ioActionContext, VolumeType volumeType) {
      // if primary down, stop write
      boolean createSuccess = false;
      if (ioActionContext.isPrimaryDown()) {
        logger.error("if primary down, can not sent to members in this:{}", this);
      } else {
        // send PJ or P(if joining secondary down)
        if (goodPrimaryCount > 0) {
          if (ioActionContext.isJoiningSecondaryDown()) {
            createSuccess = true;
          } else {
            if (goodJoiningSecondariesCount > 0) {
              createSuccess = true;
            }
          }
        }
      }
      return createSuccess;
    }

    @Override
    public boolean mergeReadLogResult(int goodPrimaryCount,
        SecondariesCountInfo secondariesCountInfo,
        int goodJoiningSecondariesCount, int goodArbiterCount, IoActionContext ioActionContext,
        VolumeType volumeType) {
      // must read from P
      boolean readSuccess = false;
      if (goodPrimaryCount > 0) {
        readSuccess = true;
      }
      return readSuccess;
    }

    @Override
    public boolean mergeCommitLogResult(int goodPrimaryCount, int goodSecondariesCount,
        int goodJoiningSecondariesCount, IoActionContext ioActionContext, VolumeType volumeType) {
      // if primary down, stop write
      boolean canCommit = false;
      if (ioActionContext.isPrimaryDown()) {
        logger.error("if primary down, can not sent to members in this:{}", this);
      } else {
        // send PJ or P(if joining secondary down)
        if (goodPrimaryCount > 0) {
          if (ioActionContext.isJoiningSecondaryDown()) {
            canCommit = true;
          } else {
            if (goodJoiningSecondariesCount > 0) {
              canCommit = true;
            }
          }
        }
      }
      return canCommit;
    }

    @Override
    public void processWriteIoActionContext(int primaryCount, int secondariesCount,
        int joiningSecondariesCount,
        IoActionContext ioActionContext, VolumeType volumeType) {
      if (primaryCount == 0) {
        ioActionContext.setResendDirectly(true);
      }
    }

    @Override
    public void processReadIoActionContext(int primaryCount, int secondariesCount,
        int joiningSecondariesCount,
        int goodArbiterCount, IoActionContext ioActionContext, VolumeType volumeType) {
      // must read from P
      if (primaryCount == 0) {
        ioActionContext.setResendDirectly(true);
      } else {
        ioActionContext.doNotNeedCheckRead();
      }
    }

    @Override
    public boolean writeDoneDirectly(int primaryDisconnectCount, int secondaryDisconnectCount,
        int joiningSecondaryDisconnectCount, int arbiterDisconnectCount, VolumeType volumeType) {
      if (primaryDisconnectCount > 0) {
        return true;
      }
      return false;
    }

    @Override
    public boolean readDoneDirectly(int primaryDisconnectCount, int secondaryDisconnectCount,
        int joiningSecondaryDisconnectCount, int arbiterDisconnectCount, VolumeType volumeType) {
      if (primaryDisconnectCount > 0) {
        return true;
      }
      return false;
    }
  },

  PJJ(5) {
    @Override
    public boolean mergeCreateLogResult(int goodPrimaryCount, int goodSecondariesCount,
        int goodJoiningSecondariesCount, IoActionContext ioActionContext, VolumeType volumeType) {
      logger.error("if primary down, can not sent to members in this:{}", this);
      return false;
    }

    @Override
    public boolean mergeReadLogResult(int goodPrimaryCount,
        SecondariesCountInfo secondariesCountInfo,
        int goodJoiningSecondariesCount, int goodArbiterCount, IoActionContext ioActionContext,
        VolumeType volumeType) {
      // must read from P
      boolean readSuccess = false;
      if (goodPrimaryCount > 0) {
        readSuccess = true;
      }
      return readSuccess;
    }

    @Override
    public boolean mergeCommitLogResult(int goodPrimaryCount, int goodSecondariesCount,
        int goodJoiningSecondariesCount, IoActionContext ioActionContext, VolumeType volumeType) {
      logger.error("if primary down, can not sent to members in this:{}", this);
      return false;
    }

    @Override
    public void processWriteIoActionContext(int primaryCount, int secondariesCount,
        int joiningSecondariesCount,
        IoActionContext ioActionContext, VolumeType volumeType) {
      ioActionContext.setResendDirectly(true);
    }

    @Override
    public void processReadIoActionContext(int primaryCount, int secondariesCount,
        int joiningSecondariesCount,
        int goodArbiterCount, IoActionContext ioActionContext, VolumeType volumeType) {
      // must read from P
      if (primaryCount == 0) {
        ioActionContext.setResendDirectly(true);
      } else {
        ioActionContext.doNotNeedCheckRead();
      }
    }

    @Override
    public boolean writeDoneDirectly(int primaryDisconnectCount, int secondaryDisconnectCount,
        int joiningSecondaryDisconnectCount, int arbiterDisconnectCount, VolumeType volumeType) {
      return true;
    }

    @Override
    public boolean readDoneDirectly(int primaryDisconnectCount, int secondaryDisconnectCount,
        int joiningSecondaryDisconnectCount, int arbiterDisconnectCount, VolumeType volumeType) {
      if (primaryDisconnectCount > 0) {
        return true;
      }
      return false;
    }
  },

  PII(6) {
    @Override
    public boolean mergeCreateLogResult(int goodPrimaryCount, int goodSecondariesCount,
        int goodJoiningSecondariesCount, IoActionContext ioActionContext, VolumeType volumeType) {
      if (ioActionContext.isPrimaryDown()) {
        logger.error("primary down at this:{}, can not write any more", this);
      } else {
        if (goodPrimaryCount > 0) {
          return true;
        }
      }
      return false;
    }

    @Override
    public boolean mergeReadLogResult(int goodPrimaryCount,
        SecondariesCountInfo secondariesCountInfo,
        int goodJoiningSecondariesCount, int goodArbiterCount, IoActionContext ioActionContext,
        VolumeType volumeType) {
      // must read from P
      boolean readSuccess = false;
      if (goodPrimaryCount > 0) {
        readSuccess = true;
      }
      return readSuccess;
    }

    @Override
    public boolean mergeCommitLogResult(int goodPrimaryCount, int goodSecondariesCount,
        int goodJoiningSecondariesCount, IoActionContext ioActionContext, VolumeType volumeType) {
      if (ioActionContext.isPrimaryDown()) {
        logger.error("primary down at this:{}, can not write any more", this);
      } else {
        if (goodPrimaryCount > 0) {
          return true;
        }
      }
      return false;
    }

    @Override
    public void processWriteIoActionContext(int primaryCount, int secondariesCount,
        int joiningSecondariesCount,
        IoActionContext ioActionContext, VolumeType volumeType) {
      if (primaryCount == 0) {
        ioActionContext.setResendDirectly(true);
      }
    }

    @Override
    public void processReadIoActionContext(int primaryCount, int secondariesCount,
        int joiningSecondariesCount,
        int goodArbiterCount, IoActionContext ioActionContext, VolumeType volumeType) {
      // must read from P
      if (primaryCount == 0) {
        ioActionContext.setResendDirectly(true);
      } else {
        ioActionContext.doNotNeedCheckRead();
      }
    }

    @Override
    public boolean writeDoneDirectly(int primaryDisconnectCount, int secondaryDisconnectCount,
        int joiningSecondaryDisconnectCount, int arbiterDisconnectCount, VolumeType volumeType) {
      if (primaryDisconnectCount > 0) {
        return true;
      }
      return false;
    }

    @Override
    public boolean readDoneDirectly(int primaryDisconnectCount, int secondaryDisconnectCount,
        int joiningSecondaryDisconnectCount, int arbiterDisconnectCount, VolumeType volumeType) {
      if (primaryDisconnectCount > 0) {
        return true;
      }
      return false;
    }

    @Override
    public boolean onlyPrimary() {
      return true;
    }
  },

  PS(7) {
    @Override
    public boolean mergeCreateLogResult(int goodPrimaryCount, int goodSecondariesCount,
        int goodJoiningSecondariesCount, IoActionContext ioActionContext, VolumeType volumeType) {
      // if primary down, stop write
      boolean createSuccess = false;
      if (ioActionContext.isPrimaryDown()) {
        logger.error("if primary down, can not sent to members in this:{}", this);
      } else {
        // send PS or P(if secondary down)
        if (goodPrimaryCount > 0) {
          if (ioActionContext.isSecondaryDown()) {
            createSuccess = true;
          } else {
            if (goodSecondariesCount > 0) {
              createSuccess = true;
            }
          }
        }
      }
      return createSuccess;
    }

    @Override
    public boolean mergeReadLogResult(int goodPrimaryCount,
        SecondariesCountInfo secondariesCountInfo,
        int goodJoiningSecondariesCount, int goodArbiterCount, IoActionContext ioActionContext,
        VolumeType volumeType) {
      // if primary down(but no way to mark primary down), still can read from S
      boolean readSuccess = false;
      if (!ioActionContext.isPrimaryDown()) {
        if (goodPrimaryCount > 0) {
          readSuccess = true;
        }
      }
      return readSuccess;
    }

    @Override
    public boolean mergeCommitLogResult(int goodPrimaryCount, int goodSecondariesCount,
        int goodJoiningSecondariesCount, IoActionContext ioActionContext, VolumeType volumeType) {
      // if primary down, stop write
      boolean canCommit = false;
      if (ioActionContext.isPrimaryDown()) {
        logger.error("if primary down, can not sent to members in this:{}", this);
      } else {
        // send PS or P(if secondary down)
        if (goodPrimaryCount > 0) {
          if (ioActionContext.isSecondaryDown()) {
            canCommit = true;
          } else {
            if (goodSecondariesCount > 0) {
              canCommit = true;
            }
          }
        }
      }
      return canCommit;
    }

    @Override
    public void processWriteIoActionContext(int primaryCount, int secondariesCount,
        int joiningSecondariesCount,
        IoActionContext ioActionContext, VolumeType volumeType) {
      if (primaryCount == 0) {
        ioActionContext.setResendDirectly(true);
      }
    }

    @Override
    public void processReadIoActionContext(int primaryCount, int secondariesCount,
        int joiningSecondariesCount,
        int goodArbiterCount, IoActionContext ioActionContext, VolumeType volumeType) {
      // if primary down(but no way to mark primary down), still can read from S
      if (primaryCount == 0) {
        ioActionContext.setResendDirectly(true);
      } else {
        ioActionContext.doNotNeedCheckRead();
      }
    }

    @Override
    public boolean writeDoneDirectly(int primaryDisconnectCount, int secondaryDisconnectCount,
        int joiningSecondaryDisconnectCount, int arbiterDisconnectCount, VolumeType volumeType) {
      if (primaryDisconnectCount > 0) {
        return true;
      }
      return false;
    }

    @Override
    public boolean readDoneDirectly(int primaryDisconnectCount, int secondaryDisconnectCount,
        int joiningSecondaryDisconnectCount, int arbiterDisconnectCount, VolumeType volumeType) {
      if (primaryDisconnectCount > 0) {
        return true;
      }
      return false;
    }
  },

  PJ(8) {
    @Override
    public boolean mergeCreateLogResult(int goodPrimaryCount, int goodSecondariesCount,
        int goodJoiningSecondariesCount, IoActionContext ioActionContext, VolumeType volumeType) {
      // if primary down, stop write
      boolean createSuccess = false;
      if (ioActionContext.isPrimaryDown()) {
        logger.error("if primary down, can not sent to members in this:{}", this);
      } else {
        // send PJ or P(if J down)
        if (goodPrimaryCount > 0) {
          if (ioActionContext.isJoiningSecondaryDown()) {
            createSuccess = true;
          } else {
            if (goodJoiningSecondariesCount > 0) {
              createSuccess = true;
            }
          }
        }
      }
      return createSuccess;
    }

    @Override
    public boolean mergeReadLogResult(int goodPrimaryCount,
        SecondariesCountInfo secondariesCountInfo,
        int goodJoiningSecondariesCount, int goodArbiterCount, IoActionContext ioActionContext,
        VolumeType volumeType) {
      // must read from primary
      boolean readSuccess = false;
      if (goodPrimaryCount > 0) {
        readSuccess = true;
      }
      return readSuccess;
    }

    @Override
    public boolean mergeCommitLogResult(int goodPrimaryCount, int goodSecondariesCount,
        int goodJoiningSecondariesCount, IoActionContext ioActionContext, VolumeType volumeType) {
      // if primary down, stop write
      boolean canCommit = false;
      if (ioActionContext.isPrimaryDown()) {
        logger.error("if primary down, can not sent to members in this:{}", this);
      } else {
        // send PJ or P(if J down)
        if (goodPrimaryCount > 0) {
          if (ioActionContext.isJoiningSecondaryDown()) {
            canCommit = true;
          } else {
            if (goodJoiningSecondariesCount > 0) {
              canCommit = true;
            }
          }
        }
      }
      return canCommit;
    }

    @Override
    public void processWriteIoActionContext(int primaryCount, int secondariesCount,
        int joiningSecondariesCount,
        IoActionContext ioActionContext, VolumeType volumeType) {
      if (primaryCount == 0) {
        ioActionContext.setResendDirectly(true);
      }
    }

    @Override
    public void processReadIoActionContext(int primaryCount, int secondariesCount,
        int joiningSecondariesCount,
        int goodArbiterCount, IoActionContext ioActionContext, VolumeType volumeType) {
      // if primary down, still can read from S
      if (primaryCount == 0) {
        ioActionContext.setResendDirectly(true);
      } else {
        ioActionContext.doNotNeedCheckRead();
      }
    }

    @Override
    public boolean writeDoneDirectly(int primaryDisconnectCount, int secondaryDisconnectCount,
        int joiningSecondaryDisconnectCount, int arbiterDisconnectCount, VolumeType volumeType) {
      if (primaryDisconnectCount > 0) {
        return true;
      }
      return false;
    }

    @Override
    public boolean readDoneDirectly(int primaryDisconnectCount, int secondaryDisconnectCount,
        int joiningSecondaryDisconnectCount, int arbiterDisconnectCount, VolumeType volumeType) {
      if (primaryDisconnectCount > 0) {
        return true;
      }
      return false;
    }
  },

  PI(9) {
    @Override
    public boolean mergeCreateLogResult(int goodPrimaryCount, int goodSecondariesCount,
        int goodJoiningSecondariesCount, IoActionContext ioActionContext, VolumeType volumeType) {
      if (ioActionContext.isPrimaryDown()) {
        logger.error("primary is down at:{}, can not write any more", this);
      } else {
        if (goodPrimaryCount > 0) {
          return true;
        }
      }
      return false;
    }

    @Override
    public boolean mergeReadLogResult(int goodPrimaryCount,
        SecondariesCountInfo secondariesCountInfo,
        int goodJoiningSecondariesCount, int goodArbiterCount, IoActionContext ioActionContext,
        VolumeType volumeType) {
      // must read from primary
      boolean readSuccess = false;
      if (goodPrimaryCount > 0) {
        readSuccess = true;
      }
      return readSuccess;
    }

    @Override
    public boolean mergeCommitLogResult(int goodPrimaryCount, int goodSecondariesCount,
        int goodJoiningSecondariesCount, IoActionContext ioActionContext, VolumeType volumeType) {
      if (ioActionContext.isPrimaryDown()) {
        logger.error("primary is down at:{}, can not write any more", this);
      } else {
        if (goodPrimaryCount > 0) {
          return true;
        }
      }
      return false;
    }

    @Override
    public void processWriteIoActionContext(int primaryCount, int secondariesCount,
        int joiningSecondariesCount,
        IoActionContext ioActionContext, VolumeType volumeType) {
      if (primaryCount == 0) {
        ioActionContext.setResendDirectly(true);
      }
    }

    @Override
    public void processReadIoActionContext(int primaryCount, int secondariesCount,
        int joiningSecondariesCount,
        int goodArbiterCount, IoActionContext ioActionContext, VolumeType volumeType) {
      // if primary down, still can read from S
      if (primaryCount == 0) {
        ioActionContext.setResendDirectly(true);
      }
    }

    @Override
    public boolean writeDoneDirectly(int primaryDisconnectCount, int secondaryDisconnectCount,
        int joiningSecondaryDisconnectCount, int arbiterDisconnectCount, VolumeType volumeType) {
      if (primaryDisconnectCount > 0) {
        return true;
      }
      return false;
    }

    @Override
    public boolean readDoneDirectly(int primaryDisconnectCount, int secondaryDisconnectCount,
        int joiningSecondaryDisconnectCount, int arbiterDisconnectCount, VolumeType volumeType) {
      if (primaryDisconnectCount > 0) {
        return true;
      }
      return false;
    }

    @Override
    public boolean onlyPrimary() {
      return true;
    }
  },

  PSA(10) {
    @Override
    public boolean mergeCreateLogResult(int goodPrimaryCount, int goodSecondariesCount,
        int goodJoiningSecondariesCount, IoActionContext ioActionContext, VolumeType volumeType) {
      // if primary down, write to S
      boolean createSuccess = false;
      if (ioActionContext.isPrimaryDown()) {
        if (goodSecondariesCount > 0) {
          createSuccess = true;
        }
      } else {
        // send PS or S
        if (goodPrimaryCount > 0) {
          if (ioActionContext.isSecondaryDown()) {
            createSuccess = true;
          } else {
            if (goodSecondariesCount > 0) {
              createSuccess = true;
            }
          }
        }
      }
      return createSuccess;
    }

    @Override
    public boolean mergeReadLogResult(int goodPrimaryCount,
        SecondariesCountInfo secondariesCountInfo,
        int goodJoiningSecondariesCount, int goodArbiterCount, IoActionContext ioActionContext,
        VolumeType volumeType) {
      boolean readSuccess = false;
      int goodSecondariesCount = secondariesCountInfo.allSecondariesCount();
      // read from PSA or PS or PA or SA
      if (ioActionContext.isPrimaryDown()) {
        if (goodSecondariesCount + goodArbiterCount >= volumeType.getWriteQuorumSize()) {
          readSuccess = true;
        }
      } else {
        if (goodPrimaryCount > 0
            && (goodPrimaryCount + goodSecondariesCount + goodArbiterCount) >= volumeType
            .getWriteQuorumSize()) {
          readSuccess = true;
        }
      }

      return readSuccess;
    }

    @Override
    public boolean mergeCommitLogResult(int goodPrimaryCount, int goodSecondariesCount,
        int goodJoiningSecondariesCount, IoActionContext ioActionContext, VolumeType volumeType) {
      // if primary down, stop write
      boolean canCommit = false;
      if (ioActionContext.isPrimaryDown()) {
        // sent to S
        if (goodSecondariesCount > 0) {
          canCommit = true;
        }
      } else {
        // send PS or S
        if (goodPrimaryCount > 0) {
          if (ioActionContext.isSecondaryDown()) {
            canCommit = true;
          } else {
            if (goodSecondariesCount > 0) {
              canCommit = true;
            }
          }
        }
      }
      return canCommit;
    }

    @Override
    public void processWriteIoActionContext(int primaryCount, int secondariesCount,
        int joiningSecondariesCount,
        IoActionContext ioActionContext, VolumeType volumeType) {
      if (primaryCount + secondariesCount == 0) {
        ioActionContext.setResendDirectly(true);
      }
    }

    @Override
    public void processReadIoActionContext(int primaryCount, int secondariesCount,
        int joiningSecondariesCount,
        int goodArbiterCount, IoActionContext ioActionContext, VolumeType volumeType) {
      // if primary down, still can read from S
      // read PSA or PS or PA or SA
      // read from PSA or PS or PA or SA
      if (ioActionContext.isPrimaryDown()) {
        if (secondariesCount + goodArbiterCount < volumeType.getWriteQuorumSize()) {
          ioActionContext.setResendDirectly(true);
        }
      } else {
        if (primaryCount == 0 || (primaryCount + secondariesCount + goodArbiterCount) < volumeType
            .getWriteQuorumSize()) {
          ioActionContext.setResendDirectly(true);
        }
      }
    }

    @Override
    public boolean writeDoneDirectly(int primaryDisconnectCount, int secondaryDisconnectCount,
        int joiningSecondaryDisconnectCount, int arbiterDisconnectCount, VolumeType volumeType) {
      if (primaryDisconnectCount + secondaryDisconnectCount >= volumeType.getWriteQuorumSize()) {
        return true;
      }
      return false;
    }

    @Override
    public boolean readDoneDirectly(int primaryDisconnectCount, int secondaryDisconnectCount,
        int joiningSecondaryDisconnectCount, int arbiterDisconnectCount, VolumeType volumeType) {
      if (primaryDisconnectCount + secondaryDisconnectCount + arbiterDisconnectCount >= volumeType
          .getWriteQuorumSize()) {
        return true;
      }
      return false;
    }

    @Override
    public boolean canGenerateNewPrimary() {
      return true;
    }

    /**
     * if 3 members voting, but only PA in the group, another alive S is missing, then new
     * membership is pai after become primary one inactive S is the alive S.
     * and then we kill the p, and the S can not voting with the A to get a new p.
     * so it is not safe to becomePrimary with the S missing.
     */
    @Override
    public boolean safeToBecomePrimaryWithAsecondaryMissing() {
      return false;
    }

  },

  PJA(11) {
    @Override
    public boolean mergeCreateLogResult(int goodPrimaryCount, int goodSecondariesCount,
        int goodJoiningSecondariesCount, IoActionContext ioActionContext, VolumeType volumeType) {
      // if primary down, stop write
      boolean createSuccess = false;
      if (ioActionContext.isPrimaryDown()) {
        logger.error("if primary down, can not sent to members in this:{}", this);
      } else {
        // send PJ or P(if J down)
        if (goodPrimaryCount > 0) {
          if (ioActionContext.isJoiningSecondaryDown()) {
            createSuccess = true;
          } else {
            if (goodJoiningSecondariesCount > 0) {
              createSuccess = true;
            }
          }
        }
      }
      return createSuccess;
    }

    @Override
    public boolean mergeReadLogResult(int goodPrimaryCount,
        SecondariesCountInfo secondariesCountInfo,
        int goodJoiningSecondariesCount, int goodArbiterCount, IoActionContext ioActionContext,
        VolumeType volumeType) {
      boolean readSuccess = false;
      // read from P
      if (goodPrimaryCount > 0) {
        readSuccess = true;
      }
      return readSuccess;
    }

    @Override
    public boolean mergeCommitLogResult(int goodPrimaryCount, int goodSecondariesCount,
        int goodJoiningSecondariesCount, IoActionContext ioActionContext, VolumeType volumeType) {
      // if primary down, stop write
      boolean canCommit = false;
      if (ioActionContext.isPrimaryDown()) {
        logger.error("if primary down, can not sent to members in this:{}", this);
      } else {
        // send PJ or P(if J down)
        if (goodPrimaryCount > 0) {
          if (ioActionContext.isJoiningSecondaryDown()) {
            canCommit = true;
          } else {
            if (goodJoiningSecondariesCount > 0) {
              canCommit = true;
            }
          }
        }
      }
      return canCommit;
    }

    @Override
    public void processWriteIoActionContext(int primaryCount, int secondariesCount,
        int joiningSecondariesCount,
        IoActionContext ioActionContext, VolumeType volumeType) {
      if (primaryCount == 0) {
        ioActionContext.setResendDirectly(true);
      }
    }

    @Override
    public void processReadIoActionContext(int primaryCount, int secondariesCount,
        int joiningSecondariesCount,
        int goodArbiterCount, IoActionContext ioActionContext, VolumeType volumeType) {
      // only read P
      if (primaryCount == 0) {
        ioActionContext.setResendDirectly(true);
      } else {
        ioActionContext.doNotNeedCheckRead();
      }
    }

    @Override
    public boolean writeDoneDirectly(int primaryDisconnectCount, int secondaryDisconnectCount,
        int joiningSecondaryDisconnectCount, int arbiterDisconnectCount, VolumeType volumeType) {
      if (primaryDisconnectCount > 0) {
        return true;
      }
      return false;
    }

    @Override
    public boolean readDoneDirectly(int primaryDisconnectCount, int secondaryDisconnectCount,
        int joiningSecondaryDisconnectCount, int arbiterDisconnectCount, VolumeType volumeType) {
      if (primaryDisconnectCount > 0) {
        return true;
      }
      return false;
    }

  },

  PIA(12) {
    @Override
    public boolean mergeCreateLogResult(int goodPrimaryCount, int goodSecondariesCount,
        int goodJoiningSecondariesCount, IoActionContext ioActionContext, VolumeType volumeType) {
      // if primary down, stop write
      boolean createSuccess = false;
      if (ioActionContext.isPrimaryDown()) {
        logger.error("if primary down, can not sent to members in this:{}", this);
      } else {
        // send P
        if (goodPrimaryCount > 0) {
          createSuccess = true;
        }
      }
      return createSuccess;
    }

    @Override
    public boolean mergeReadLogResult(int goodPrimaryCount,
        SecondariesCountInfo secondariesCountInfo,
        int goodJoiningSecondariesCount, int goodArbiterCount, IoActionContext ioActionContext,
        VolumeType volumeType) {
      boolean readSuccess = false;
      // read from P
      if (goodPrimaryCount > 0) {
        readSuccess = true;
      }
      return readSuccess;
    }

    @Override
    public boolean mergeCommitLogResult(int goodPrimaryCount, int goodSecondariesCount,
        int goodJoiningSecondariesCount, IoActionContext ioActionContext, VolumeType volumeType) {
      // if primary down, stop write
      boolean canCommit = false;
      if (ioActionContext.isPrimaryDown()) {
        logger.error("if primary down, can not sent to members in this:{}", this);
      } else {
        // send P
        if (goodPrimaryCount > 0) {
          canCommit = true;
        }
      }
      return canCommit;
    }

    @Override
    public void processWriteIoActionContext(int primaryCount, int secondariesCount,
        int joiningSecondariesCount,
        IoActionContext ioActionContext, VolumeType volumeType) {
      if (primaryCount == 0) {
        ioActionContext.setResendDirectly(true);
      }
    }

    @Override
    public void processReadIoActionContext(int primaryCount, int secondariesCount,
        int joiningSecondariesCount,
        int goodArbiterCount, IoActionContext ioActionContext, VolumeType volumeType) {
      // only read P
      if (primaryCount == 0) {
        ioActionContext.setResendDirectly(true);
      } else {
        ioActionContext.doNotNeedCheckRead();
      }
    }

    @Override
    public boolean writeDoneDirectly(int primaryDisconnectCount, int secondaryDisconnectCount,
        int joiningSecondaryDisconnectCount, int arbiterDisconnectCount, VolumeType volumeType) {
      if (primaryDisconnectCount > 0) {
        return true;
      }
      return false;
    }

    @Override
    public boolean readDoneDirectly(int primaryDisconnectCount, int secondaryDisconnectCount,
        int joiningSecondaryDisconnectCount, int arbiterDisconnectCount, VolumeType volumeType) {
      if (primaryDisconnectCount > 0) {
        return true;
      }
      return false;
    }

    @Override
    public boolean onlyPrimary() {
      return true;
    }
  },

  PA(13) {
    @Override
    public boolean mergeCreateLogResult(int goodPrimaryCount, int goodSecondariesCount,
        int goodJoiningSecondariesCount, IoActionContext ioActionContext, VolumeType volumeType) {
      // if primary down, stop write
      boolean createSuccess = false;
      if (ioActionContext.isPrimaryDown()) {
        logger.error("if primary down, can not sent to members in this:{}", this);
      } else {
        // send P
        if (goodPrimaryCount > 0) {
          createSuccess = true;
        }
      }
      return createSuccess;
    }

    @Override
    public boolean mergeReadLogResult(int goodPrimaryCount,
        SecondariesCountInfo secondariesCountInfo,
        int goodJoiningSecondariesCount, int goodArbiterCount, IoActionContext ioActionContext,
        VolumeType volumeType) {
      boolean readSuccess = false;
      // read from P
      if (ioActionContext.isPrimaryDown()) {
        logger.error("if primary down, can not sent to members in this:{}", this);
      } else {
        if (goodPrimaryCount > 0) {
          readSuccess = true;
        }
      }
      return readSuccess;
    }

    @Override
    public boolean mergeCommitLogResult(int goodPrimaryCount, int goodSecondariesCount,
        int goodJoiningSecondariesCount, IoActionContext ioActionContext, VolumeType volumeType) {
      // if primary down, stop write
      boolean canCommit = false;
      if (ioActionContext.isPrimaryDown()) {
        logger.error("if primary down, can not sent to members in this:{}", this);
      } else {
        // send P
        if (goodPrimaryCount > 0) {
          canCommit = true;
        }
      }
      return canCommit;
    }

    @Override
    public void processWriteIoActionContext(int primaryCount, int secondariesCount,
        int joiningSecondariesCount,
        IoActionContext ioActionContext, VolumeType volumeType) {
      if (primaryCount == 0) {
        ioActionContext.setResendDirectly(true);
      }
    }

    @Override
    public void processReadIoActionContext(int primaryCount, int secondariesCount,
        int joiningSecondariesCount,
        int goodArbiterCount, IoActionContext ioActionContext, VolumeType volumeType) {
      // only read P
      if (primaryCount == 0) {
        ioActionContext.setResendDirectly(true);
      } else {
        ioActionContext.doNotNeedCheckRead();
      }
    }

    @Override
    public boolean writeDoneDirectly(int primaryDisconnectCount, int secondaryDisconnectCount,
        int joiningSecondaryDisconnectCount, int arbiterDisconnectCount, VolumeType volumeType) {
      if (primaryDisconnectCount > 0) {
        return true;
      }
      return false;
    }

    @Override
    public boolean readDoneDirectly(int primaryDisconnectCount, int secondaryDisconnectCount,
        int joiningSecondaryDisconnectCount, int arbiterDisconnectCount, VolumeType volumeType) {
      if (primaryDisconnectCount > 0) {
        return true;
      }
      return false;
    }

    @Override
    public boolean onlyPrimary() {
      return true;
    }
  },

  TPS(14) {
    @Override
    public boolean mergeCreateLogResult(int goodPrimaryCount, int goodSecondariesCount,
        int goodJoiningSecondariesCount, IoActionContext ioActionContext, VolumeType volumeType) {
      boolean createSuccess = false;
      if (ioActionContext.isPrimaryDown()) {
        logger.warn("primary is down at:{}, so can't write any more", this);
      } else {
        // send PS or P(if S down)
        if (goodPrimaryCount > 0) {
          if (ioActionContext.isSecondaryDown()) {
            createSuccess = true;
          } else {
            if (goodSecondariesCount > 0) {
              createSuccess = true;
            }
          }
        }
      }
      return createSuccess;
    }

    @Override
    public boolean mergeReadLogResult(int goodPrimaryCount,
        SecondariesCountInfo secondariesCountInfo,
        int goodJoiningSecondariesCount, int goodArbiterCount, IoActionContext ioActionContext,
        VolumeType volumeType) {
      // if primary down(but no way to mark primary down), still can read from S
      boolean readSuccess = false;
      if (!ioActionContext.isPrimaryDown()) {
        if (goodPrimaryCount > 0) {
          readSuccess = true;
        }
      }
      return readSuccess;
    }

    @Override
    public boolean mergeCommitLogResult(int goodPrimaryCount, int goodSecondariesCount,
        int goodJoiningSecondariesCount, IoActionContext ioActionContext, VolumeType volumeType) {
      boolean canCommit = false;
      if (ioActionContext.isPrimaryDown()) {
        logger.warn("primary is down at:{}, so can't write any more", this);
      } else {
        // send PS or P(if S down)
        if (goodPrimaryCount > 0) {
          if (ioActionContext.isSecondaryDown()) {
            canCommit = true;
          } else {
            if (goodSecondariesCount > 0) {
              canCommit = true;
            }
          }
        }
      }
      return canCommit;
    }

    @Override
    public void processWriteIoActionContext(int primaryCount, int secondariesCount,
        int joiningSecondariesCount,
        IoActionContext ioActionContext, VolumeType volumeType) {
      if (primaryCount == 0) {
        ioActionContext.setResendDirectly(true);
      }
    }

    @Override
    public void processReadIoActionContext(int primaryCount, int secondariesCount,
        int joiningSecondariesCount,
        int goodArbiterCount, IoActionContext ioActionContext, VolumeType volumeType) {
      // if primary down(but no way to mark primary down), still can read from S
      if (primaryCount == 0) {
        ioActionContext.setResendDirectly(true);
      } else {
        ioActionContext.doNotNeedCheckRead();
      }
    }

    @Override
    public boolean writeDoneDirectly(int primaryDisconnectCount, int secondaryDisconnectCount,
        int joiningSecondaryDisconnectCount, int arbiterDisconnectCount, VolumeType volumeType) {
      if (primaryDisconnectCount > 0) {
        return true;
      }
      return false;
    }

    @Override
    public boolean readDoneDirectly(int primaryDisconnectCount, int secondaryDisconnectCount,
        int joiningSecondaryDisconnectCount, int arbiterDisconnectCount, VolumeType volumeType) {
      if (primaryDisconnectCount > 0) {
        return true;
      }
      return false;
    }

  },

  TPJ(15) {
    @Override
    public boolean mergeCreateLogResult(int goodPrimaryCount, int goodSecondariesCount,
        int goodJoiningSecondariesCount, IoActionContext ioActionContext, VolumeType volumeType) {
      // if primary down, stop write
      boolean createSuccess = false;
      if (ioActionContext.isPrimaryDown()) {
        logger.error("if primary down, can not sent to members in this:{}", this);
      } else {
        // send PJ or P(if J down)
        if (goodPrimaryCount > 0) {
          if (ioActionContext.isJoiningSecondaryDown()) {
            createSuccess = true;
          } else {
            if (goodJoiningSecondariesCount > 0) {
              createSuccess = true;
            }
          }
        }
      }
      return createSuccess;
    }

    @Override
    public boolean mergeReadLogResult(int goodPrimaryCount,
        SecondariesCountInfo secondariesCountInfo,
        int goodJoiningSecondariesCount, int goodArbiterCount, IoActionContext ioActionContext,
        VolumeType volumeType) {
      boolean readSuccess = false;
      // read from P
      if (goodPrimaryCount > 0) {
        readSuccess = true;
      }
      return readSuccess;
    }

    @Override
    public boolean mergeCommitLogResult(int goodPrimaryCount, int goodSecondariesCount,
        int goodJoiningSecondariesCount, IoActionContext ioActionContext, VolumeType volumeType) {
      // if primary down, stop write
      boolean canCommit = false;
      if (ioActionContext.isPrimaryDown()) {
        logger.error("if primary down, can not sent to members in this:{}", this);
      } else {
        // send PJ or P(if J down)
        if (goodPrimaryCount > 0) {
          if (ioActionContext.isJoiningSecondaryDown()) {
            canCommit = true;
          } else {
            if (goodJoiningSecondariesCount > 0) {
              canCommit = true;
            }
          }
        }
      }
      return canCommit;
    }

    @Override
    public void processWriteIoActionContext(int primaryCount, int secondariesCount,
        int joiningSecondariesCount,
        IoActionContext ioActionContext, VolumeType volumeType) {
      if (primaryCount == 0) {
        ioActionContext.setResendDirectly(true);
      }
    }

    @Override
    public void processReadIoActionContext(int primaryCount, int secondariesCount,
        int joiningSecondariesCount,
        int goodArbiterCount, IoActionContext ioActionContext, VolumeType volumeType) {
      // only read P
      if (primaryCount == 0) {
        ioActionContext.setResendDirectly(true);
      } else {
        ioActionContext.doNotNeedCheckRead();
      }
    }

    @Override
    public boolean writeDoneDirectly(int primaryDisconnectCount, int secondaryDisconnectCount,
        int joiningSecondaryDisconnectCount, int arbiterDisconnectCount, VolumeType volumeType) {
      if (primaryDisconnectCount > 0) {
        return true;
      }
      return false;
    }

    @Override
    public boolean readDoneDirectly(int primaryDisconnectCount, int secondaryDisconnectCount,
        int joiningSecondaryDisconnectCount, int arbiterDisconnectCount, VolumeType volumeType) {
      if (primaryDisconnectCount > 0) {
        return true;
      }
      return false;
    }
  },

  TPI(16) {
    @Override
    public boolean mergeCreateLogResult(int goodPrimaryCount, int goodSecondariesCount,
        int goodJoiningSecondariesCount, IoActionContext ioActionContext, VolumeType volumeType) {
      // if primary down, stop write
      boolean createSuccess = false;
      if (ioActionContext.isPrimaryDown()) {
        logger.error("if primary down, can not sent to members in this:{}", this);
      } else {
        // send P
        if (goodPrimaryCount > 0) {
          createSuccess = true;
        }
      }
      return createSuccess;
    }

    @Override
    public boolean mergeReadLogResult(int goodPrimaryCount,
        SecondariesCountInfo secondariesCountInfo,
        int goodJoiningSecondariesCount, int goodArbiterCount, IoActionContext ioActionContext,
        VolumeType volumeType) {
      boolean readSuccess = false;
      // read from P
      if (goodPrimaryCount > 0) {
        readSuccess = true;
      }
      return readSuccess;
    }

    @Override
    public boolean mergeCommitLogResult(int goodPrimaryCount, int goodSecondariesCount,
        int goodJoiningSecondariesCount, IoActionContext ioActionContext, VolumeType volumeType) {
      // if primary down, stop write
      boolean canCommit = false;
      if (ioActionContext.isPrimaryDown()) {
        logger.error("if primary down, can not sent to members in this:{}", this);
      } else {
        // send P
        if (goodPrimaryCount > 0) {
          canCommit = true;
        }
      }
      return canCommit;
    }

    @Override
    public void processWriteIoActionContext(int primaryCount, int secondariesCount,
        int joiningSecondariesCount,
        IoActionContext ioActionContext, VolumeType volumeType) {
      if (primaryCount == 0) {
        ioActionContext.setResendDirectly(true);
      }
    }

    @Override
    public void processReadIoActionContext(int primaryCount, int secondariesCount,
        int joiningSecondariesCount,
        int goodArbiterCount, IoActionContext ioActionContext, VolumeType volumeType) {
      // only read P
      if (primaryCount == 0) {
        ioActionContext.setResendDirectly(true);
      } else {
        ioActionContext.doNotNeedCheckRead();
      }
    }

    @Override
    public boolean writeDoneDirectly(int primaryDisconnectCount, int secondaryDisconnectCount,
        int joiningSecondaryDisconnectCount, int arbiterDisconnectCount, VolumeType volumeType) {
      if (primaryDisconnectCount > 0) {
        return true;
      }
      return false;
    }

    @Override
    public boolean readDoneDirectly(int primaryDisconnectCount, int secondaryDisconnectCount,
        int joiningSecondaryDisconnectCount, int arbiterDisconnectCount, VolumeType volumeType) {
      if (primaryDisconnectCount > 0) {
        return true;
      }
      return false;
    }

    @Override
    public boolean onlyPrimary() {
      return true;
    }
  },

  PSSAA(17) {
    @Override
    public boolean mergeReadLogResult(int goodPrimaryCount,
        SecondariesCountInfo secondariesCountInfo,
        int goodJoiningSecondariesCount, int goodArbiterCount, IoActionContext ioActionContext,
        VolumeType volumeType) {
      boolean readSuccess = false;
      int goodSecondariesCount = secondariesCountInfo.allSecondariesCount();
      // if primary down, should sent to SS
      if (ioActionContext.isPrimaryDown()) {
        if (secondariesCountInfo.getFetchCount() > 0
            && goodSecondariesCount + goodArbiterCount >= volumeType.getWriteQuorumSize()) {
          readSuccess = true;
        }
      } else {
        // send P+ (S or A the number >2) PSA PSSAA
        if (goodPrimaryCount > 0 && goodSecondariesCount + goodArbiterCount > 1) {
          readSuccess = true;
        }
      }
      return readSuccess;
    }

    @Override
    public void processWriteIoActionContext(int primaryCount, int secondariesCount,
        int joiningSecondariesCount,
        IoActionContext ioActionContext, VolumeType volumeType) {
      // must p and s down
      if (primaryCount == 0 && secondariesCount == 0) {
        logger.warn("primary and secondaries down, processWriteIOActionContext in this:{}", this);
        ioActionContext.setResendDirectly(true);
      }
    }

    @Override
    public void processReadIoActionContext(int primaryCount, int secondariesCount,
        int joiningSecondariesCount,
        int goodArbiterCount, IoActionContext ioActionContext, VolumeType volumeType) {
      // determine read could success or not
      //
      if (primaryCount == 0) {
        // can not read any more
        if (secondariesCount + goodArbiterCount < volumeType.getWriteQuorumSize()) {
          ioActionContext.setResendDirectly(true);
        }

      } else if (primaryCount > 0) {
        //PS PA
        if (secondariesCount + goodArbiterCount < 2) {
          ioActionContext.setResendDirectly(true);
        }
      } else {
        Validate.isTrue(false);
      }
    }

    @Override
    public boolean writeDoneDirectly(int primaryDisconnectCount, int secondaryDisconnectCount,
        int joiningSecondaryDisconnectCount, int arbiterDisconnectCount, VolumeType volumeType) {
      if (primaryDisconnectCount + secondaryDisconnectCount + arbiterDisconnectCount >= volumeType
          .getWriteQuorumSize()) {
        return true;
      }
      return false;
    }

    @Override
    public boolean readDoneDirectly(int primaryDisconnectCount, int secondaryDisconnectCount,
        int joiningSecondaryDisconnectCount, int arbiterDisconnectCount, VolumeType volumeType) {
      //dead 3
      if (primaryDisconnectCount + secondaryDisconnectCount + arbiterDisconnectCount >= volumeType
          .getWriteQuorumSize()) {
        return true;
      }
      return false;
    }

    @Override
    public boolean canGenerateNewPrimary() {
      return true;
    }

    /**
     * if P died, and the left 4 members voting, but only SAA in the group, another alive S is
     * missing, then new membership is paaii after become primary
     * one inactive S is old P, the other is the alive S. if the new P die again, the alive S can
     * not be new P
     * and then we kill the p, and the i back, the i can not voting with the A to get a new p.
     * so it is not safe to becomePrimary with the S missing.
     */
    @Override
    public boolean safeToBecomePrimaryWithAsecondaryMissing() {
      return false;
    }
  },

  PSSAI(18) {
    @Override
    public boolean mergeReadLogResult(int goodPrimaryCount,
        SecondariesCountInfo secondariesCountInfo, int goodJoiningSecondariesCount,
        int goodArbiterCount, IoActionContext ioActionContext, VolumeType volumeType) {
      boolean readSuccess = false;
      int goodSecondariesCount = secondariesCountInfo.allSecondariesCount();
      // if primary down, should sent to SS
      if (ioActionContext.isPrimaryDown()) {
        if (goodSecondariesCount + goodArbiterCount >= volumeType.getWriteQuorumSize()
            && secondariesCountInfo.getFetchCount() > 0) {
          readSuccess = true;
        }
      } else {
        // send P+ (S or A the number >2) PSA PSSAA
        if (goodPrimaryCount > 0 && goodSecondariesCount + goodArbiterCount > 1) {
          readSuccess = true;
        }
      }
      return readSuccess;
    }

    @Override
    public void processWriteIoActionContext(int primaryCount, int secondariesCount,
        int joiningSecondariesCount,
        IoActionContext ioActionContext, VolumeType volumeType) {
      // determine write could success or not
      if (primaryCount == 0) {
        //SSA
        // can not write any more
        if (secondariesCount < 2) {
          ioActionContext.setResendDirectly(true);
        }
      } else if (primaryCount > 0) {
        if (secondariesCount == 0) {
          ioActionContext.setResendDirectly(true);
        }
      }
    }

    @Override
    public void processReadIoActionContext(int primaryCount, int secondariesCount,
        int joiningSecondariesCount,
        int goodArbiterCount, IoActionContext ioActionContext, VolumeType volumeType) {
      // determine read could success or not
      //
      if (primaryCount == 0) {
        //SSA
        if (secondariesCount + goodArbiterCount < volumeType.getWriteQuorumSize()) {
          ioActionContext.setResendDirectly(true);
        }
      } else if (primaryCount > 0) {
        //PS PA
        if (secondariesCount + goodArbiterCount < 2) {
          ioActionContext.setResendDirectly(true);
        }
      } else {
        Validate.isTrue(false);
      }
    }

    @Override
    public boolean writeDoneDirectly(int primaryDisconnectCount, int secondaryDisconnectCount,
        int joiningSecondaryDisconnectCount, int arbiterDisconnectCount, VolumeType volumeType) {
      //dead 2
      if (primaryDisconnectCount + secondaryDisconnectCount + arbiterDisconnectCount
          >= volumeType.getWriteQuorumSize() - 1) {
        return true;
      }
      return false;
    }

    @Override
    public boolean readDoneDirectly(int primaryDisconnectCount, int secondaryDisconnectCount,
        int joiningSecondaryDisconnectCount, int arbiterDisconnectCount, VolumeType volumeType) {
      //dead 2
      if (primaryDisconnectCount + secondaryDisconnectCount + arbiterDisconnectCount
          >= volumeType.getWriteQuorumSize() - 1) {
        return true;
      }
      return false;
    }

    @Override
    public boolean canGenerateNewPrimary() {
      return true;
    }

  },

  PSSII(19) {
    @Override
    public boolean mergeReadLogResult(int goodPrimaryCount,
        SecondariesCountInfo secondariesCountInfo,
        int goodJoiningSecondariesCount, int goodArbiterCount, IoActionContext ioActionContext,
        VolumeType volumeType) {
      boolean readSuccess = false;
      int goodSecondariesCount = secondariesCountInfo.allSecondariesCount();
      // if primary down, should sent to SS
      if (ioActionContext.isPrimaryDown()) {
        readSuccess = false;

      } else {
        // only PSS
        if (goodPrimaryCount > 0 && goodSecondariesCount > 1) {
          readSuccess = true;
        }
      }
      return readSuccess;
    }

    @Override
    public void processWriteIoActionContext(int primaryCount, int secondariesCount,
        int joiningSecondariesCount,
        IoActionContext ioActionContext, VolumeType volumeType) {
      // determine write could success or not
      if (primaryCount == 0) {
        //SSAA
        ioActionContext.setResendDirectly(true);
      } else if (primaryCount > 0) {
        if (secondariesCount < 2) {
          ioActionContext.setResendDirectly(true);
        }
      }

    }

    @Override
    public void processReadIoActionContext(int primaryCount, int secondariesCount,
        int joiningSecondariesCount,
        int goodArbiterCount, IoActionContext ioActionContext, VolumeType volumeType) {
      if (primaryCount == 0) {
        ioActionContext.setResendDirectly(true);
      } else if (primaryCount > 0) {
        //PS P
        if (secondariesCount < 2) {
          ioActionContext.setResendDirectly(true);
        }
      }

    }

    @Override
    public boolean writeDoneDirectly(int primaryDisconnectCount, int secondaryDisconnectCount,
        int joiningSecondaryDisconnectCount, int arbiterDisconnectCount, VolumeType volumeType) {
      //dead 1
      if (primaryDisconnectCount + secondaryDisconnectCount >= 1) {
        return true;
      }
      return false;
    }

    @Override
    public boolean readDoneDirectly(int primaryDisconnectCount, int secondaryDisconnectCount,
        int joiningSecondaryDisconnectCount, int arbiterDisconnectCount, VolumeType volumeType) {
      //dead 1
      if (primaryDisconnectCount + secondaryDisconnectCount + arbiterDisconnectCount >= 1) {
        return true;
      }
      return false;
    }

    @Override
    public boolean canGenerateNewPrimary() {
      return true;
    }

  },

  PSJAA(20) {
    @Override
    public boolean mergeReadLogResult(int goodPrimaryCount,
        SecondariesCountInfo secondariesCountInfo,
        int goodJoiningSecondariesCount, int goodArbiterCount, IoActionContext ioActionContext,
        VolumeType volumeType) {
      boolean readSuccess = false;
      int goodSecondariesCount = secondariesCountInfo.allSecondariesCount();
      // if primary down, must have 1 S and (J + A > 1)
      if (ioActionContext.isPrimaryDown()) { //SJAA
        if (secondariesCountInfo.getFetchCount() > 0
            && goodJoiningSecondariesCount + goodArbiterCount > 1) {
          readSuccess = true;
        }
      } else {
        // p ok,the (S + J + A >1 )
        if (goodPrimaryCount > 0
            && goodSecondariesCount + goodArbiterCount + goodJoiningSecondariesCount > 1) {
          readSuccess = true;
        }
      }
      return readSuccess;
    }

    @Override
    public void processWriteIoActionContext(int primaryCount, int secondariesCount,
        int joiningSecondariesCount,
        IoActionContext ioActionContext, VolumeType volumeType) {
      // determine write could success or not
      if (primaryCount == 0 && secondariesCount == 0) { //SJAA
        ioActionContext.setResendDirectly(true);
      }

    }

    @Override
    public void processReadIoActionContext(int primaryCount, int secondariesCount,
        int joiningSecondariesCount,
        int goodArbiterCount, IoActionContext ioActionContext, VolumeType volumeType) {
      if (primaryCount == 0) {
        //must 1 s and (J + A < 2)
        if (secondariesCount > 0 && joiningSecondariesCount + goodArbiterCount < 2) {
          ioActionContext.setResendDirectly(true);
        }
        if (secondariesCount == 0) {
          //JAA
          ioActionContext.setResendDirectly(true);
        }

      } else if (primaryCount > 0) {
        //p + s + j < 2
        if (secondariesCount + joiningSecondariesCount + goodArbiterCount < 2) {
          ioActionContext.setResendDirectly(true);
        }
      }
    }

    @Override
    public boolean writeDoneDirectly(int primaryDisconnectCount, int secondaryDisconnectCount,
        int joiningSecondaryDisconnectCount, int arbiterDisconnectCount, VolumeType volumeType) {
      //PS
      if (primaryDisconnectCount + secondaryDisconnectCount + joiningSecondaryDisconnectCount
          + arbiterDisconnectCount >= volumeType.getWriteQuorumSize()
          || primaryDisconnectCount + secondaryDisconnectCount > 1) {
        return true;
      }
      return false;
    }

    @Override
    public boolean readDoneDirectly(int primaryDisconnectCount, int secondaryDisconnectCount,
        int joiningSecondaryDisconnectCount, int arbiterDisconnectCount, VolumeType volumeType) {
      //PS
      if (primaryDisconnectCount + secondaryDisconnectCount + joiningSecondaryDisconnectCount
          + arbiterDisconnectCount >= volumeType.getWriteQuorumSize()
          || primaryDisconnectCount + secondaryDisconnectCount > 1) {
        //JAA
        return true;
      }
      return false;
    }

    @Override
    public boolean canGenerateNewPrimary() {
      return true;
    }

  },

  PSJAI(21) {
    @Override
    public boolean mergeReadLogResult(int goodPrimaryCount,
        SecondariesCountInfo secondariesCountInfo, int goodJoiningSecondariesCount,
        int goodArbiterCount, IoActionContext ioActionContext, VolumeType volumeType) {
      boolean readSuccess = false;
      int goodSecondariesCount = secondariesCountInfo.allSecondariesCount();
      // if primary down, only SJA
      if (ioActionContext.isPrimaryDown()) {
        if (secondariesCountInfo.getFetchCount() > 0
            && goodJoiningSecondariesCount + goodArbiterCount > 1) {
          readSuccess = true;
        }

      } else {
        // p ok,the other must 2
        if (goodPrimaryCount > 0
            && goodSecondariesCount + goodJoiningSecondariesCount + goodArbiterCount > 1) {
          readSuccess = true;
        }
      }
      return readSuccess;
    }

    @Override
    public void processWriteIoActionContext(int primaryCount, int secondariesCount,
        int joiningSecondariesCount,
        IoActionContext ioActionContext, VolumeType volumeType) {
      // determine write could success or not
      //SJ
      if (primaryCount == 0) {
        // must have one s or j
        if (secondariesCount == 0) {
          ioActionContext.setResendDirectly(true);
        }

        if (secondariesCount > 0 && joiningSecondariesCount == 0) {
          ioActionContext.setResendDirectly(true);
        }
      } else if (primaryCount > 0) {
        //do not have s or j
        if (secondariesCount + joiningSecondariesCount < 1) {
          ioActionContext.setResendDirectly(true);
        }
      } else {
        Validate.isTrue(false);
      }
    }

    @Override
    public void processReadIoActionContext(int primaryCount, int secondariesCount,
        int joiningSecondariesCount,
        int goodArbiterCount, IoActionContext ioActionContext, VolumeType volumeType) {
      //have one or two? only SJA
      if (primaryCount == 0) {
        if (secondariesCount == 0) {
          ioActionContext.setResendDirectly(true);
        } else {
          if (secondariesCount > 0 && joiningSecondariesCount + goodArbiterCount < 2) {
            ioActionContext.setResendDirectly(true);
          }
        }

      } else if (primaryCount > 0) {
        //must have one
        if (secondariesCount + joiningSecondariesCount + goodArbiterCount < 2
            && secondariesCount + joiningSecondariesCount + goodArbiterCount > 0) {
          ioActionContext.setResendDirectly(true);
        }
      }
    }

    @Override
    public boolean writeDoneDirectly(int primaryDisconnectCount, int secondaryDisconnectCount,
        int joiningSecondaryDisconnectCount, int arbiterDisconnectCount, VolumeType volumeType) {
      //dead >= 2
      if (primaryDisconnectCount + secondaryDisconnectCount + joiningSecondaryDisconnectCount
          + arbiterDisconnectCount >= volumeType.getWriteQuorumSize() - 1) {
        return true;
      }
      return false;
    }

    @Override
    public boolean readDoneDirectly(int primaryDisconnectCount, int secondaryDisconnectCount,
        int joiningSecondaryDisconnectCount, int arbiterDisconnectCount, VolumeType volumeType) {
      //dead >=2
      if (primaryDisconnectCount + secondaryDisconnectCount + joiningSecondaryDisconnectCount
          + arbiterDisconnectCount >= volumeType.getWriteQuorumSize() - 1) {
        return true;
      }
      return false;
    }

    @Override
    public boolean canGenerateNewPrimary() {
      return true;
    }

  },

  PSJII(22) {
    @Override
    public boolean mergeReadLogResult(int goodPrimaryCount,
        SecondariesCountInfo secondariesCountInfo,
        int goodJoiningSecondariesCount, int goodArbiterCount, IoActionContext ioActionContext,
        VolumeType volumeType) {
      boolean readSuccess = false;
      int goodSecondariesCount = secondariesCountInfo.allSecondariesCount();
      //only PSJ
      if (goodPrimaryCount + goodSecondariesCount + goodJoiningSecondariesCount == volumeType
          .getWriteQuorumSize()) {
        readSuccess = true;
      }
      return readSuccess;
    }

    @Override
    public void processWriteIoActionContext(int primaryCount, int secondariesCount,
        int joiningSecondariesCount,
        IoActionContext ioActionContext, VolumeType volumeType) {
      //PSJ only
      if (primaryCount == 0) {
        ioActionContext.setResendDirectly(true);
      } else if (primaryCount > 0) {
        if (secondariesCount + joiningSecondariesCount < 2) {
          ioActionContext.setResendDirectly(true);
        }
      }
    }

    @Override
    public void processReadIoActionContext(int primaryCount, int secondariesCount,
        int joiningSecondariesCount,
        int goodArbiterCount, IoActionContext ioActionContext, VolumeType volumeType) {
      //PSJ only
      if (primaryCount == 0) {
        ioActionContext.setResendDirectly(true);
      } else if (primaryCount > 0) {
        if (secondariesCount + joiningSecondariesCount < 2) {
          ioActionContext.setResendDirectly(true);
        }
      } else {
        Validate.isTrue(false);
      }
    }

    @Override
    public boolean writeDoneDirectly(int primaryDisconnectCount, int secondaryDisconnectCount,
        int joiningSecondaryDisconnectCount, int arbiterDisconnectCount, VolumeType volumeType) {
      //dead one
      if (primaryDisconnectCount + secondaryDisconnectCount + joiningSecondaryDisconnectCount
          >= 1) {
        return true;
      }
      return false;
    }

    @Override
    public boolean readDoneDirectly(int primaryDisconnectCount, int secondaryDisconnectCount,
        int joiningSecondaryDisconnectCount, int arbiterDisconnectCount, VolumeType volumeType) {
      //dead one
      if (primaryDisconnectCount + secondaryDisconnectCount + joiningSecondaryDisconnectCount
          >= 1) {
        return true;
      }
      return false;
    }

    @Override
    public boolean canGenerateNewPrimary() {
      return true;
    }

  },

  PSIAA(23) {
    @Override
    public boolean mergeReadLogResult(int goodPrimaryCount,
        SecondariesCountInfo secondariesCountInfo,
        int goodJoiningSecondariesCount, int goodArbiterCount, IoActionContext ioActionContext,
        VolumeType volumeType) {
      boolean readSuccess = false;
      int goodSecondariesCount = secondariesCountInfo.allSecondariesCount();
      // if primary down, SAA
      if (ioActionContext.isPrimaryDown()) {
        if (secondariesCountInfo.getFetchCount() > 0
            && goodArbiterCount + goodSecondariesCount >= volumeType.getWriteQuorumSize()) {
          readSuccess = true;
        }

      } else {
        // send P+ (S or A the number >1) PSA PAA PSAA
        if (goodPrimaryCount > 0 && goodSecondariesCount + goodArbiterCount > 1) {
          readSuccess = true;
        }
      }
      return readSuccess;
    }

    @Override
    public void processWriteIoActionContext(int primaryCount, int secondariesCount,
        int joiningSecondariesCount,
        IoActionContext ioActionContext, VolumeType volumeType) {
      // P PS
      if ((primaryCount + secondariesCount) == 0) {
        ioActionContext.setResendDirectly(true);
      }
    }

    @Override
    public void processReadIoActionContext(int primaryCount, int secondariesCount,
        int joiningSecondariesCount,
        int goodArbiterCount, IoActionContext ioActionContext, VolumeType volumeType) {
      // determine read could success or not
      //
      if (primaryCount == 0) { //SAA
        if (secondariesCount == 0) {
          ioActionContext.setResendDirectly(true);
        }
        if (secondariesCount > 0 && goodArbiterCount < 2) {
          ioActionContext.setResendDirectly(true);
        }

      } else if (primaryCount > 0) {
        //PSA PAA
        if (secondariesCount + goodArbiterCount < 2) {
          ioActionContext.setResendDirectly(true);
        }
      } else {
        Validate.isTrue(false);
      }
    }

    @Override
    public boolean writeDoneDirectly(int primaryDisconnectCount, int secondaryDisconnectCount,
        int joiningSecondaryDisconnectCount, int arbiterDisconnectCount, VolumeType volumeType) {
      //dead >= 2
      if (primaryDisconnectCount + secondaryDisconnectCount + arbiterDisconnectCount
          >= volumeType.getWriteQuorumSize() - 1) {
        return true;
      }
      return false;
    }

    @Override
    public boolean readDoneDirectly(int primaryDisconnectCount, int secondaryDisconnectCount,
        int joiningSecondaryDisconnectCount, int arbiterDisconnectCount, VolumeType volumeType) {
      //dead >= 2
      if (primaryDisconnectCount + secondaryDisconnectCount + arbiterDisconnectCount
          >= volumeType.getWriteQuorumSize() - 1) {
        return true;
      }
      return false;
    }

    @Override
    public boolean canGenerateNewPrimary() {
      return true;
    }

  },

  PSIAI(24) {
    @Override
    public boolean mergeReadLogResult(int goodPrimaryCount,
        SecondariesCountInfo secondariesCountInfo,
        int goodJoiningSecondariesCount, int goodArbiterCount, IoActionContext ioActionContext,
        VolumeType volumeType) {
      boolean readSuccess = false;
      int goodSecondariesCount = secondariesCountInfo.allSecondariesCount();
      //only PSA
      if (goodPrimaryCount > 0 && goodSecondariesCount + goodArbiterCount > 1) {
        readSuccess = true;
      }

      return readSuccess;
    }

    @Override
    public void processWriteIoActionContext(int primaryCount, int secondariesCount,
        int joiningSecondariesCount,
        IoActionContext ioActionContext, VolumeType volumeType) {
      // determine write could success or not
      if (primaryCount == 0) {
        ioActionContext.setResendDirectly(true);

      } else if (primaryCount > 0) {
        if (secondariesCount == 0) {
          ioActionContext.setResendDirectly(true);
        }
      } else {
        Validate.isTrue(false);
      }
    }

    @Override
    public void processReadIoActionContext(int primaryCount, int secondariesCount,
        int joiningSecondariesCount,
        int goodArbiterCount, IoActionContext ioActionContext, VolumeType volumeType) {
      // determine read could success or not
      if (primaryCount == 0) {
        ioActionContext.setResendDirectly(true);
      } else if (primaryCount > 0) {
        //PS PA
        if (secondariesCount + goodArbiterCount < 2) {
          ioActionContext.setResendDirectly(true);
        }
      } else {
        Validate.isTrue(false);
      }
    }

    @Override
    public boolean writeDoneDirectly(int primaryDisconnectCount, int secondaryDisconnectCount,
        int joiningSecondaryDisconnectCount, int arbiterDisconnectCount, VolumeType volumeType) {
      //dead 1
      if (primaryDisconnectCount + secondaryDisconnectCount + arbiterDisconnectCount >= 1) {
        return true;
      }
      return false;
    }

    @Override
    public boolean readDoneDirectly(int primaryDisconnectCount, int secondaryDisconnectCount,
        int joiningSecondaryDisconnectCount, int arbiterDisconnectCount, VolumeType volumeType) {
      //dead 1
      if (primaryDisconnectCount + secondaryDisconnectCount + arbiterDisconnectCount >= 1) {
        return true;
      }
      return false;
    }

    @Override
    public boolean canGenerateNewPrimary() {
      return true;
    }

  },

  PSIII(25) {
    @Override
    public void processWriteIoActionContext(int primaryCount, int secondariesCount,
        int joiningSecondariesCount, IoActionContext ioActionContext, VolumeType volumeType) {
      ioActionContext.setResendDirectly(true);
    }

    @Override
    public void processReadIoActionContext(int primaryCount, int secondariesCount,
        int joiningSecondariesCount, int goodArbiterCount, IoActionContext ioActionContext,
        VolumeType volumeType) {
      ioActionContext.setResendDirectly(true);
    }

  },

  PJJAA(26) {
    @Override
    public boolean mergeReadLogResult(int goodPrimaryCount,
        SecondariesCountInfo secondariesCountInfo, int goodJoiningSecondariesCount,
        int goodArbiterCount, IoActionContext ioActionContext, VolumeType volumeType) {
      boolean readSuccess = false;
      //must have P and two other
      if (!ioActionContext.isPrimaryDown()
          && goodPrimaryCount > 0
          && goodArbiterCount + goodJoiningSecondariesCount > 1) {
        readSuccess = true;
      }

      return readSuccess;
    }

    @Override
    public void processWriteIoActionContext(int primaryCount, int secondariesCount,
        int joiningSecondariesCount,
        IoActionContext ioActionContext, VolumeType volumeType) {
      // determine write could success or not
      if (primaryCount == 0) {
        // can not write any more
        ioActionContext.setResendDirectly(true);
      } else if (primaryCount > 0) {
        //PJJ PJ
        if (joiningSecondariesCount == 0) {
          ioActionContext.setResendDirectly(true);
        }
      } else {
        Validate.isTrue(false);
      }
    }

    @Override
    public void processReadIoActionContext(int primaryCount, int secondariesCount,
        int joiningSecondariesCount,
        int goodArbiterCount, IoActionContext ioActionContext, VolumeType volumeType) {
      // determine read could success or not

      if (primaryCount == 0) {
        ioActionContext.setResendDirectly(true);
      } else if (primaryCount > 0) {
        //PJJAA
        if (joiningSecondariesCount + goodArbiterCount < 2) {
          ioActionContext.setResendDirectly(true);
        }
      } else {
        Validate.isTrue(false);
      }
    }

    @Override
    public boolean writeDoneDirectly(int primaryDisconnectCount, int secondaryDisconnectCount,
        int joiningSecondaryDisconnectCount, int arbiterDisconnectCount, VolumeType volumeType) {
      if (primaryDisconnectCount > 0) {
        return true;
      } else {
        if (joiningSecondaryDisconnectCount + arbiterDisconnectCount > 2) {
          return true;
        }
      }
      return false;
    }

    @Override
    public boolean readDoneDirectly(int primaryDisconnectCount, int secondaryDisconnectCount,
        int joiningSecondaryDisconnectCount, int arbiterDisconnectCount, VolumeType volumeType) {
      if (primaryDisconnectCount > 0) {
        return true;
      } else {
        if (joiningSecondaryDisconnectCount + arbiterDisconnectCount > 2) {
          return true;
        }
      }
      return false;
    }

  },

  PJJAI(27) {
    @Override
    public boolean mergeReadLogResult(int goodPrimaryCount,
        SecondariesCountInfo secondariesCountInfo,
        int goodJoiningSecondariesCount, int goodArbiterCount, IoActionContext ioActionContext,
        VolumeType volumeType) {
      boolean readSuccess = false;
      //must have P PJJA
      if (goodPrimaryCount > 0 && goodJoiningSecondariesCount + goodArbiterCount > 1) {
        readSuccess = true;
      }

      return readSuccess;
    }

    @Override
    public void processWriteIoActionContext(int primaryCount, int secondariesCount,
        int joiningSecondariesCount,
        IoActionContext ioActionContext, VolumeType volumeType) {
      // determine write could success or not
      if (primaryCount == 0) {
        ioActionContext.setResendDirectly(true);
      } else if (primaryCount > 0) {
        if (joiningSecondariesCount == 0) {
          ioActionContext.setResendDirectly(true);
        }
      } else {
        Validate.isTrue(false);
      }
    }

    @Override
    public void processReadIoActionContext(int primaryCount, int secondariesCount,
        int joiningSecondariesCount,
        int goodArbiterCount, IoActionContext ioActionContext, VolumeType volumeType) {
      // determine read could success or not
      //
      if (primaryCount == 0) {
        ioActionContext.setResendDirectly(true);

      } else if (primaryCount > 0) {
        //PJA PJJ
        if (joiningSecondariesCount + goodArbiterCount < 2) {
          ioActionContext.setResendDirectly(true);
        }
      } else {
        Validate.isTrue(false);
      }
    }

    @Override
    public boolean writeDoneDirectly(int primaryDisconnectCount, int secondaryDisconnectCount,
        int joiningSecondaryDisconnectCount, int arbiterDisconnectCount, VolumeType volumeType) {
      if (primaryDisconnectCount > 0) {
        return true;
      } else {
        if (joiningSecondaryDisconnectCount + arbiterDisconnectCount > 1) {
          return true;
        }
      }
      return false;
    }

    @Override
    public boolean readDoneDirectly(int primaryDisconnectCount, int secondaryDisconnectCount,
        int joiningSecondaryDisconnectCount, int arbiterDisconnectCount, VolumeType volumeType) {
      if (primaryDisconnectCount > 0) {
        return true;
      } else {
        if (joiningSecondaryDisconnectCount + arbiterDisconnectCount > 1) {
          return true;
        }
      }
      return false;

    }

  },

  PJJII(28) {
    @Override
    public boolean mergeReadLogResult(int goodPrimaryCount,
        SecondariesCountInfo secondariesCountInfo,
        int goodJoiningSecondariesCount, int goodArbiterCount, IoActionContext ioActionContext,
        VolumeType volumeType) {
      boolean readSuccess = false;
      //must PJJ
      if (goodPrimaryCount > 0 && goodJoiningSecondariesCount > 1) {
        readSuccess = true;
      }
      return readSuccess;
    }

    @Override
    public void processWriteIoActionContext(int primaryCount, int secondariesCount,
        int joiningSecondariesCount,
        IoActionContext ioActionContext, VolumeType volumeType) {
      // determine write could success or not
      if (primaryCount == 0) {
        ioActionContext.setResendDirectly(true);
      } else if (primaryCount > 0) {
        //PJJ
        if (joiningSecondariesCount < 2) {
          ioActionContext.setResendDirectly(true);
        }
      } else {
        Validate.isTrue(false);
      }
    }

    @Override
    public void processReadIoActionContext(int primaryCount, int secondariesCount,
        int joiningSecondariesCount,
        int goodArbiterCount, IoActionContext ioActionContext, VolumeType volumeType) {
      // determine read could success or not
      if (primaryCount == 0) {
        ioActionContext.setResendDirectly(true);
      } else if (primaryCount > 0) {
        //PJJ
        if (joiningSecondariesCount < 2) {
          ioActionContext.setResendDirectly(true);
        }
      } else {
        Validate.isTrue(false);
      }
    }

    @Override
    public boolean writeDoneDirectly(int primaryDisconnectCount, int secondaryDisconnectCount,
        int joiningSecondaryDisconnectCount, int arbiterDisconnectCount, VolumeType volumeType) {
      //dead 1
      if (primaryDisconnectCount + joiningSecondaryDisconnectCount >= 1) {
        return true;
      }
      return false;
    }

    @Override
    public boolean readDoneDirectly(int primaryDisconnectCount, int secondaryDisconnectCount,
        int joiningSecondaryDisconnectCount, int arbiterDisconnectCount, VolumeType volumeType) {
      //dead 1
      if (primaryDisconnectCount + joiningSecondaryDisconnectCount >= 1) {
        return true;
      }
      return false;
    }

  },

  PJIAA(29) {
    @Override
    public boolean mergeReadLogResult(int goodPrimaryCount,
        SecondariesCountInfo secondariesCountInfo,
        int goodJoiningSecondariesCount, int goodArbiterCount, IoActionContext ioActionContext,
        VolumeType volumeType) {
      boolean readSuccess = false;
      //must have P
      if (goodPrimaryCount > 0 && goodJoiningSecondariesCount + goodArbiterCount > 1) {
        readSuccess = true;
      }
      return readSuccess;
    }

    @Override
    public void processWriteIoActionContext(int primaryCount, int secondariesCount,
        int joiningSecondariesCount,
        IoActionContext ioActionContext, VolumeType volumeType) {
      // dead 1
      if (primaryCount == 0 || joiningSecondariesCount == 0) {
        ioActionContext.setResendDirectly(true);
      }

    }

    @Override
    public void processReadIoActionContext(int primaryCount, int secondariesCount,
        int joiningSecondariesCount,
        int goodArbiterCount, IoActionContext ioActionContext, VolumeType volumeType) {
      // determine write could success or not
      if (primaryCount == 0) {
        ioActionContext.setResendDirectly(true);
      } else {
        if (primaryCount > 0 && joiningSecondariesCount + goodArbiterCount < 2) {
          ioActionContext.setResendDirectly(true);
        }
      }
    }

    @Override
    public boolean writeDoneDirectly(int primaryDisconnectCount, int secondaryDisconnectCount,
        int joiningSecondaryDisconnectCount, int arbiterDisconnectCount, VolumeType volumeType) {
      if (primaryDisconnectCount > 0
          || joiningSecondaryDisconnectCount + arbiterDisconnectCount >= 2) {
        return true;
      }
      return false;
    }

    @Override
    public boolean readDoneDirectly(int primaryDisconnectCount, int secondaryDisconnectCount,
        int joiningSecondaryDisconnectCount, int arbiterDisconnectCount, VolumeType volumeType) {
      if (primaryDisconnectCount > 0
          || joiningSecondaryDisconnectCount + arbiterDisconnectCount >= 2) {
        return true;
      }
      return false;
    }

    @Override
    public boolean canGenerateNewPrimary() {
      return false;
    }

  },

  PJIAI(30) {
    @Override
    public boolean mergeReadLogResult(int goodPrimaryCount,
        SecondariesCountInfo secondariesCountInfo,
        int goodJoiningSecondariesCount, int goodArbiterCount, IoActionContext ioActionContext,
        VolumeType volumeType) {
      boolean readSuccess = false;
      //must have P
      if (goodPrimaryCount > 0 && goodJoiningSecondariesCount + goodArbiterCount > 1) {
        readSuccess = true;
      }
      return readSuccess;
    }

    @Override
    public void processWriteIoActionContext(int primaryCount, int secondariesCount,
        int joiningSecondariesCount,
        IoActionContext ioActionContext, VolumeType volumeType) {
      // PJ
      if (primaryCount == 0 || joiningSecondariesCount == 0) {
        ioActionContext.setResendDirectly(true);
      }
    }

    @Override
    public void processReadIoActionContext(int primaryCount, int secondariesCount,
        int joiningSecondariesCount,
        int goodArbiterCount, IoActionContext ioActionContext, VolumeType volumeType) {
      //
      if (primaryCount == 0) {
        ioActionContext.setResendDirectly(true);
      } else { //PJI
        if (primaryCount > 0 && joiningSecondariesCount + goodArbiterCount < 2) {
          ioActionContext.setResendDirectly(true);
        }
      }
    }

    @Override
    public boolean writeDoneDirectly(int primaryDisconnectCount, int secondaryDisconnectCount,
        int joiningSecondaryDisconnectCount, int arbiterDisconnectCount, VolumeType volumeType) {
      //dead 1
      if (primaryDisconnectCount + joiningSecondaryDisconnectCount + arbiterDisconnectCount >= 1) {
        return true;
      }
      return false;
    }

    @Override
    public boolean readDoneDirectly(int primaryDisconnectCount, int secondaryDisconnectCount,
        int joiningSecondaryDisconnectCount, int arbiterDisconnectCount, VolumeType volumeType) {
      //dead 1
      if (primaryDisconnectCount + joiningSecondaryDisconnectCount + arbiterDisconnectCount >= 1) {
        return true;
      }
      return false;
    }

  },

  PJIII(31) {
    @Override
    public void processWriteIoActionContext(int primaryCount, int secondariesCount,
        int joiningSecondariesCount, IoActionContext ioActionContext, VolumeType volumeType) {
      ioActionContext.setResendDirectly(true);
    }

    @Override
    public void processReadIoActionContext(int primaryCount, int secondariesCount,
        int joiningSecondariesCount, int goodArbiterCount, IoActionContext ioActionContext,
        VolumeType volumeType) {
      ioActionContext.setResendDirectly(true);
    }
  },

  PIIAA(32) {
    @Override
    public boolean mergeReadLogResult(int goodPrimaryCount,
        SecondariesCountInfo secondariesCountInfo,
        int goodJoiningSecondariesCount, int goodArbiterCount, IoActionContext ioActionContext,
        VolumeType volumeType) {
      boolean readSuccess = false;
      //must have P
      if (goodPrimaryCount > 0 && goodJoiningSecondariesCount + goodArbiterCount > 1) {
        readSuccess = true;
      }
      return readSuccess;
    }

    @Override
    public void processWriteIoActionContext(int primaryCount, int secondariesCount,
        int joiningSecondariesCount,
        IoActionContext ioActionContext, VolumeType volumeType) {
      // P
      if (primaryCount == 0) {
        ioActionContext.setResendDirectly(true);
      }
    }

    @Override
    public void processReadIoActionContext(int primaryCount, int secondariesCount,
        int joiningSecondariesCount,
        int goodArbiterCount, IoActionContext ioActionContext, VolumeType volumeType) {
      //
      if (primaryCount == 0) {
        ioActionContext.setResendDirectly(true);
      } else { //PAA
        if (primaryCount > 0 && goodArbiterCount < 2) {
          ioActionContext.setResendDirectly(true);
        }
      }
    }

    @Override
    public boolean writeDoneDirectly(int primaryDisconnectCount, int secondaryDisconnectCount,
        int joiningSecondaryDisconnectCount, int arbiterDisconnectCount, VolumeType volumeType) {
      //dead 1
      if (primaryDisconnectCount + arbiterDisconnectCount >= 1) {
        return true;
      }
      return false;
    }

    @Override
    public boolean readDoneDirectly(int primaryDisconnectCount, int secondaryDisconnectCount,
        int joiningSecondaryDisconnectCount, int arbiterDisconnectCount, VolumeType volumeType) {
      //dead 1
      if (primaryDisconnectCount + arbiterDisconnectCount >= 1) {
        return true;
      }
      return false;
    }

    @Override
    public boolean onlyPrimary() {
      return true;
    }

  },

  PIIAI(33) {
    @Override
    public void processWriteIoActionContext(int primaryCount, int secondariesCount,
        int joiningSecondariesCount, IoActionContext ioActionContext, VolumeType volumeType) {
      ioActionContext.setResendDirectly(true);
    }

    @Override
    public void processReadIoActionContext(int primaryCount, int secondariesCount,
        int joiningSecondariesCount, int goodArbiterCount, IoActionContext ioActionContext,
        VolumeType volumeType) {
      ioActionContext.setResendDirectly(true);
    }

  },

  PIIII(34) {
    @Override
    public void processWriteIoActionContext(int primaryCount, int secondariesCount,
        int joiningSecondariesCount, IoActionContext ioActionContext, VolumeType volumeType) {
      ioActionContext.setResendDirectly(true);
    }

    @Override
    public void processReadIoActionContext(int primaryCount, int secondariesCount,
        int joiningSecondariesCount, int goodArbiterCount, IoActionContext ioActionContext,
        VolumeType volumeType) {
      ioActionContext.setResendDirectly(true);
    }

  },

  PSAA(35) {
    @Override
    public boolean mergeReadLogResult(int goodPrimaryCount,
        SecondariesCountInfo secondariesCountInfo,
        int goodJoiningSecondariesCount, int goodArbiterCount, IoActionContext ioActionContext,
        VolumeType volumeType) {
      boolean readSuccess = false;
      int goodSecondariesCount = secondariesCountInfo.allSecondariesCount();
      // if primary down, should sent to ASS
      if (ioActionContext.isPrimaryDown()) {
        if (secondariesCountInfo.getFetchCount() + goodArbiterCount >= volumeType
            .getWriteQuorumSize()) {
          readSuccess = true;
        }
      } else {
        // send P+ (S or A the number >2) PSA PSAA
        if (goodPrimaryCount > 0 && goodSecondariesCount + goodArbiterCount > 1) {
          readSuccess = true;
        }
      }
      return readSuccess;
    }

    @Override
    public void processWriteIoActionContext(int primaryCount, int secondariesCount,
        int joiningSecondariesCount,
        IoActionContext ioActionContext, VolumeType volumeType) {
      // not have p and s
      if (primaryCount == 0 && secondariesCount == 0) {
        ioActionContext.setResendDirectly(true);
      }
    }

    @Override
    public void processReadIoActionContext(int primaryCount, int secondariesCount,
        int joiningSecondariesCount,
        int goodArbiterCount, IoActionContext ioActionContext, VolumeType volumeType) {
      // not have p and s
      if (primaryCount == 0) {
        if (secondariesCount == 0) {
          ioActionContext.setResendDirectly(true);
        } else {
          if (secondariesCount > 0 && goodArbiterCount < 2) {
            ioActionContext.setResendDirectly(true);
          }
        }

      } else if (primaryCount > 0) {
        //PSA  PAA
        if (secondariesCount + goodArbiterCount < 2) {
          ioActionContext.setResendDirectly(true);
        }
      } else {
        Validate.isTrue(false);
      }
    }

    @Override
    public boolean writeDoneDirectly(int primaryDisconnectCount, int secondaryDisconnectCount,
        int joiningSecondaryDisconnectCount, int arbiterDisconnectCount, VolumeType volumeType) {
      //dead 2 P and S
      if (primaryDisconnectCount + secondaryDisconnectCount + arbiterDisconnectCount
          >= volumeType.getWriteQuorumSize() - 1) {
        return true;
      }
      return false;
    }

    @Override
    public boolean readDoneDirectly(int primaryDisconnectCount, int secondaryDisconnectCount,
        int joiningSecondaryDisconnectCount, int arbiterDisconnectCount, VolumeType volumeType) {
      //dead 2
      if (primaryDisconnectCount + secondaryDisconnectCount + arbiterDisconnectCount
          >= volumeType.getWriteQuorumSize() - 1) {
        return true;
      }
      return false;
    }

    @Override
    public boolean canGenerateNewPrimary() {
      return true;
    }

  },

  PSAI(36) {
    @Override
    public boolean mergeReadLogResult(int goodPrimaryCount,
        SecondariesCountInfo secondariesCountInfo,
        int goodJoiningSecondariesCount, int goodArbiterCount, IoActionContext ioActionContext,
        VolumeType volumeType) {
      boolean readSuccess = false;
      int goodSecondariesCount = secondariesCountInfo.allSecondariesCount();
      // send P+ (S or A the number >2) PSA
      if (goodPrimaryCount > 0 && goodSecondariesCount + goodArbiterCount > 1) {
        readSuccess = true;
      }
      return readSuccess;
    }

    @Override
    public void processWriteIoActionContext(int primaryCount, int secondariesCount,
        int joiningSecondariesCount,
        IoActionContext ioActionContext, VolumeType volumeType) {
      // determine write could success or not
      if (primaryCount == 0) {
        ioActionContext.setResendDirectly(true);
      } else if (primaryCount > 0) {
        if (secondariesCount == 0) {
          ioActionContext.setResendDirectly(true);
        }
      } else {
        Validate.isTrue(false);
      }
    }

    @Override
    public void processReadIoActionContext(int primaryCount, int secondariesCount,
        int joiningSecondariesCount,
        int goodArbiterCount, IoActionContext ioActionContext, VolumeType volumeType) {
      // determine read could success or not
      //
      if (primaryCount == 0) {
        // can not read any more
        ioActionContext.setResendDirectly(true);

      } else if (primaryCount > 0 && secondariesCount + goodArbiterCount < 2) {
        //PSA
        ioActionContext.setResendDirectly(true);
      }
    }

    @Override
    public boolean writeDoneDirectly(int primaryDisconnectCount, int secondaryDisconnectCount,
        int joiningSecondaryDisconnectCount, int arbiterDisconnectCount, VolumeType volumeType) {
      //dead 1
      if (primaryDisconnectCount + secondaryDisconnectCount + arbiterDisconnectCount >= 1) {
        return true;
      }
      return false;
    }

    @Override
    public boolean readDoneDirectly(int primaryDisconnectCount, int secondaryDisconnectCount,
        int joiningSecondaryDisconnectCount, int arbiterDisconnectCount, VolumeType volumeType) {
      //dead 1
      if (primaryDisconnectCount + secondaryDisconnectCount + arbiterDisconnectCount >= 1) {
        return true;
      }
      return false;
    }

    @Override
    public boolean canGenerateNewPrimary() {
      return true;
    }

  },

  PSII(37) {
    @Override
    public void processWriteIoActionContext(int primaryCount, int secondariesCount,
        int joiningSecondariesCount, IoActionContext ioActionContext, VolumeType volumeType) {
      ioActionContext.setResendDirectly(true);
    }

    @Override
    public void processReadIoActionContext(int primaryCount, int secondariesCount,
        int joiningSecondariesCount, int goodArbiterCount, IoActionContext ioActionContext,
        VolumeType volumeType) {
      ioActionContext.setResendDirectly(true);
    }

  },

  PIAA(38) {
    @Override
    public boolean mergeReadLogResult(int goodPrimaryCount,
        SecondariesCountInfo secondariesCountInfo,
        int goodJoiningSecondariesCount, int goodArbiterCount, IoActionContext ioActionContext,
        VolumeType volumeType) {
      boolean readSuccess = false;
      //must PAA
      if (goodPrimaryCount > 0 && goodArbiterCount > 1) {
        readSuccess = true;
      }
      return readSuccess;
    }

    @Override
    public void processWriteIoActionContext(int primaryCount, int secondariesCount,
        int joiningSecondariesCount,
        IoActionContext ioActionContext, VolumeType volumeType) {
      // determine write could success or not
      if (primaryCount == 0) {
        ioActionContext.setResendDirectly(true);
      }

    }

    @Override
    public void processReadIoActionContext(int primaryCount, int secondariesCount,
        int joiningSecondariesCount,
        int goodArbiterCount, IoActionContext ioActionContext, VolumeType volumeType) {
      // determine read could success or not

      if (primaryCount == 0) {
        ioActionContext.setResendDirectly(true);
      } else {
        if (primaryCount > 0 && goodArbiterCount < 2) {
          //PAA
          ioActionContext.setResendDirectly(true);
        }
      }
    }

    @Override
    public boolean writeDoneDirectly(int primaryDisconnectCount, int secondaryDisconnectCount,
        int joiningSecondaryDisconnectCount, int arbiterDisconnectCount, VolumeType volumeType) {
      //dead 1
      if (primaryDisconnectCount + arbiterDisconnectCount > 0) {
        return true;
      }
      return false;
    }

    @Override
    public boolean readDoneDirectly(int primaryDisconnectCount, int secondaryDisconnectCount,
        int joiningSecondaryDisconnectCount, int arbiterDisconnectCount, VolumeType volumeType) {
      //dead 1
      if (primaryDisconnectCount + arbiterDisconnectCount > 0) {
        return true;
      }
      return false;
    }

    @Override
    public boolean onlyPrimary() {
      return true;
    }

  },

  PIAI(39) {
    @Override
    public void processWriteIoActionContext(int primaryCount, int secondariesCount,
        int joiningSecondariesCount, IoActionContext ioActionContext, VolumeType volumeType) {
      ioActionContext.setResendDirectly(true);
    }

    @Override
    public void processReadIoActionContext(int primaryCount, int secondariesCount,
        int joiningSecondariesCount, int goodArbiterCount, IoActionContext ioActionContext,
        VolumeType volumeType) {
      ioActionContext.setResendDirectly(true);
    }

  },

  PIII(40) {
    @Override
    public void processWriteIoActionContext(int primaryCount, int secondariesCount,
        int joiningSecondariesCount, IoActionContext ioActionContext, VolumeType volumeType) {
      ioActionContext.setResendDirectly(true);
    }

    @Override
    public void processReadIoActionContext(int primaryCount, int secondariesCount,
        int joiningSecondariesCount, int goodArbiterCount, IoActionContext ioActionContext,
        VolumeType volumeType) {
      ioActionContext.setResendDirectly(true);
    }

  },

  PJAA(41) {
    @Override
    public boolean mergeReadLogResult(int goodPrimaryCount,
        SecondariesCountInfo secondariesCountInfo,
        int goodJoiningSecondariesCount, int goodArbiterCount, IoActionContext ioActionContext,
        VolumeType volumeType) {
      boolean readSuccess = false;
      //must have P P(JAA)/P(JA)/P(AA)
      if (goodPrimaryCount > 0 && goodJoiningSecondariesCount + goodArbiterCount > 1) {
        readSuccess = true;

      }
      return readSuccess;
    }

    @Override
    public void processWriteIoActionContext(int primaryCount, int secondariesCount,
        int joiningSecondariesCount,
        IoActionContext ioActionContext, VolumeType volumeType) {
      // determine write could success or not
      if (primaryCount == 0) {
        ioActionContext.setResendDirectly(true);
      }
    }

    @Override
    public void processReadIoActionContext(int primaryCount, int secondariesCount,
        int joiningSecondariesCount,
        int goodArbiterCount, IoActionContext ioActionContext, VolumeType volumeType) {
      // determine read could success or not
      //
      if (primaryCount == 0) {
        ioActionContext.setResendDirectly(true);
      } else if (primaryCount > 0) {
        // P(JAA)/P(JA)/P(AA)
        if (joiningSecondariesCount + goodArbiterCount < 2) {
          ioActionContext.setResendDirectly(true);
        }
      }
    }

    @Override
    public boolean writeDoneDirectly(int primaryDisconnectCount, int secondaryDisconnectCount,
        int joiningSecondaryDisconnectCount, int arbiterDisconnectCount, VolumeType volumeType) {
      if (primaryDisconnectCount > 0
          || joiningSecondaryDisconnectCount + arbiterDisconnectCount >= 2) {
        return true;
      }
      return false;
    }

    @Override
    public boolean readDoneDirectly(int primaryDisconnectCount, int secondaryDisconnectCount,
        int joiningSecondaryDisconnectCount, int arbiterDisconnectCount, VolumeType volumeType) {
      if (primaryDisconnectCount > 0
          || joiningSecondaryDisconnectCount + arbiterDisconnectCount >= 2) {
        return true;
      }
      return false;
    }

  },

  PJAI(42) {
    @Override
    public boolean mergeReadLogResult(int goodPrimaryCount,
        SecondariesCountInfo secondariesCountInfo,
        int goodJoiningSecondariesCount, int goodArbiterCount, IoActionContext ioActionContext,
        VolumeType volumeType) {
      boolean readSuccess = false;
      // PJA
      if (goodPrimaryCount > 0 && goodJoiningSecondariesCount + goodArbiterCount > 1) {
        readSuccess = true;
      }
      return readSuccess;
    }

    @Override
    public void processWriteIoActionContext(int primaryCount, int secondariesCount,
        int joiningSecondariesCount,
        IoActionContext ioActionContext, VolumeType volumeType) {
      // determine write could success or not
      if (primaryCount == 0) {
        ioActionContext.setResendDirectly(true);
      } else if (primaryCount > 0) {
        if (joiningSecondariesCount == 0) {
          ioActionContext.setResendDirectly(true);
        }
      }
    }

    @Override
    public void processReadIoActionContext(int primaryCount, int secondariesCount,
        int joiningSecondariesCount,
        int goodArbiterCount, IoActionContext ioActionContext, VolumeType volumeType) {
      //can not read any more
      if (primaryCount == 0) {
        ioActionContext.setResendDirectly(true);
      } else if (primaryCount > 0) {
        //PSJ
        if (goodArbiterCount + joiningSecondariesCount < 2) {
          ioActionContext.setResendDirectly(true);
        }
      }
    }

    @Override
    public boolean writeDoneDirectly(int primaryDisconnectCount, int secondaryDisconnectCount,
        int joiningSecondaryDisconnectCount, int arbiterDisconnectCount, VolumeType volumeType) {
      //dead 1
      if (primaryDisconnectCount + joiningSecondaryDisconnectCount + arbiterDisconnectCount >= 1) {
        return true;
      }
      return false;
    }

    @Override
    public boolean readDoneDirectly(int primaryDisconnectCount, int secondaryDisconnectCount,
        int joiningSecondaryDisconnectCount, int arbiterDisconnectCount, VolumeType volumeType) {
      //dead 1
      if (primaryDisconnectCount + arbiterDisconnectCount + joiningSecondaryDisconnectCount >= 1) {
        return true;
      }
      return false;
    }

    @Override
    public boolean canGenerateNewPrimary() {
      return false;
    }

  },

  PJII(43) {
    @Override
    public void processWriteIoActionContext(int primaryCount, int secondariesCount,
        int joiningSecondariesCount, IoActionContext ioActionContext, VolumeType volumeType) {
      ioActionContext.setResendDirectly(true);
    }

    @Override
    public void processReadIoActionContext(int primaryCount, int secondariesCount,
        int joiningSecondariesCount, int goodArbiterCount, IoActionContext ioActionContext,
        VolumeType volumeType) {
      ioActionContext.setResendDirectly(true);
    }

  },

  PAA(44) {
    @Override
    public boolean mergeReadLogResult(int goodPrimaryCount,
        SecondariesCountInfo secondariesCountInfo,
        int goodJoiningSecondariesCount, int goodArbiterCount, IoActionContext ioActionContext,
        VolumeType volumeType) {
      boolean readSuccess = false;

      //PAA
      if (goodPrimaryCount > 0 && goodArbiterCount > 1) {
        readSuccess = true;
      }
      return readSuccess;
    }

    @Override
    public void processWriteIoActionContext(int primaryCount, int secondariesCount,
        int joiningSecondariesCount,
        IoActionContext ioActionContext, VolumeType volumeType) {
      // determine write could success or not
      if (primaryCount == 0) {
        ioActionContext.setResendDirectly(true);
      }
    }

    @Override
    public void processReadIoActionContext(int primaryCount, int secondariesCount,
        int joiningSecondariesCount,
        int goodArbiterCount, IoActionContext ioActionContext, VolumeType volumeType) {
      // determine read could success or not
      //
      if (primaryCount == 0) {
        // can not read any more
        ioActionContext.setResendDirectly(true);
      } else if (primaryCount > 0) {
        //PS PA
        if (goodArbiterCount < 2) {
          ioActionContext.setResendDirectly(true);
        }
      }
    }

    @Override
    public boolean writeDoneDirectly(int primaryDisconnectCount, int secondaryDisconnectCount,
        int joiningSecondaryDisconnectCount, int arbiterDisconnectCount, VolumeType volumeType) {
      if (primaryDisconnectCount + arbiterDisconnectCount > 0) {
        return true;
      }
      return false;
    }

    @Override
    public boolean readDoneDirectly(int primaryDisconnectCount, int secondaryDisconnectCount,
        int joiningSecondaryDisconnectCount, int arbiterDisconnectCount, VolumeType volumeType) {
      //dead 1
      if (primaryDisconnectCount + arbiterDisconnectCount >= 1) {
        return true;
      }
      return false;
    }

    @Override
    public boolean canGenerateNewPrimary() {
      return false;
    }

    @Override
    public boolean onlyPrimary() {
      return true;
    }

  };

  private static final Logger logger = LoggerFactory.getLogger(SegmentForm.class);
  private int value;

  SegmentForm(int value) {
    this.value = value;
  }

  public static SegmentForm findByName(String name) {
    SegmentForm segmentForm = null;
    switch (name) {
      case "PSS":
        segmentForm = PSS;
        break;
      case "PSJ":
        segmentForm = PSJ;
        break;
      case "PSI":
        segmentForm = PSI;
        break;
      case "PJI":
        segmentForm = PJI;
        break;
      case "PJJ":
        segmentForm = PJJ;
        break;
      case "PII":
        segmentForm = PII;
        break;
      case "PS":
        segmentForm = PS;
        break;
      case "PJ":
        segmentForm = PJ;
        break;
      case "PI":
        segmentForm = PI;
        break;
      case "PSA":
        segmentForm = PSA;
        break;
      case "PJA":
        segmentForm = PJA;
        break;
      case "PIA":
        segmentForm = PIA;
        break;
      case "PA":
        segmentForm = PA;
        break;
      case "TPS":
        segmentForm = TPS;
        break;
      case "TPJ":
        segmentForm = TPJ;
        break;
      case "TPI":
        segmentForm = TPI;
        break;

      default:
        Validate.isTrue(false, "unknown name:" + name);
        break;

    }
    return segmentForm;
  }

  public static SegmentForm getSegmentForm(SegmentMembership membership, VolumeType volumeType) {
    // segment form order: TWO_COPIES(IF) > PRIMARY > SECONDARY > JOINING SECONDARY
    // > INACTIVE SECONDARY > ARBITER
    SegmentForm segmentForm;
    InstanceId primary = membership.getPrimary();
    if (primary == null) {
      Validate.isTrue(false, "primary can not be null");
    }

    segmentForm = volumeType.getSegmentForm(membership.getSecondaries().size(),
        membership.getJoiningSecondaries().size(),
        membership.getArbiters().size(), membership.getInactiveSecondaries().size());
    if (segmentForm == null) {
      Validate.isTrue(false, "invalid segment form. the value is empty" + " , at:" + membership);
    }
    return segmentForm;
  }

  public boolean mergeCreateLogResult(int goodPrimaryCount, int goodSecondariesCount,
      int goodJoiningSecondariesCount,
      IoActionContext ioActionContext, VolumeType volumeType) {
    boolean createSuccess = false;
    // must all write success
    if (goodPrimaryCount + goodSecondariesCount + goodJoiningSecondariesCount == ioActionContext
        .getTotalWriteCount()) {
      createSuccess = true;
    }
    return createSuccess;
  }

  public boolean mergeReadLogResult(int goodPrimaryCount, SecondariesCountInfo secondariesCountInfo,
      int goodJoiningSecondariesCount,
      int goodArbiterCount, IoActionContext ioActionContext, VolumeType volumeType) {
    return false;
  }

  public boolean mergeCommitLogResult(int goodPrimaryCount, int goodSecondariesCount,
      int goodJoiningSecondariesCount,
      IoActionContext ioActionContext, VolumeType volumeType) {
    boolean canCommit = false;
    // must all write success
    if (goodPrimaryCount + goodSecondariesCount + goodJoiningSecondariesCount == ioActionContext
        .getTotalWriteCount()) {
      canCommit = true;
    }
    return canCommit;
  }

  public void processWriteIoActionContext(int primaryCount, int secondariesCount,
      int joiningSecondariesCount,
      IoActionContext ioActionContext, VolumeType volumeType) {
  }

  public void processReadIoActionContext(int primaryCount, int secondariesCount,
      int joiningSecondariesCount,
      int goodArbiterCount, IoActionContext ioActionContext, VolumeType volumeType) {
  }

  public boolean writable(int primaryCount, int secondariesCount, int joiningSecondariesCount,
      VolumeType volumeType) {
    IoActionContext ioActionContext = new IoActionContext();
    processWriteIoActionContext(primaryCount, secondariesCount, joiningSecondariesCount,
        ioActionContext,
        volumeType);
    return !ioActionContext.isResendDirectly();
  }

  public boolean writeDoneDirectly(int primaryDisconnectCount, int secondaryDisconnectCount,
      int joiningSecondaryDisconnectCount, int arbiterDisconnectCount, VolumeType volumeType) {
    return false;
  }

  public boolean readDoneDirectly(int primaryDisconnectCount, int secondaryDisconnectCount,
      int joiningSecondaryDisconnectCount, int arbiterDisconnectCount, VolumeType volumeType) {
    return false;
  }

  public boolean isSkipFromSourceVolumeData(int secondarySkipConut) {
    return secondarySkipConut > 0;
  }

  public boolean canGenerateNewPrimary() {
    return false;
  }

  public boolean onlyPrimary() {
    return false;
  }

  /**
   * do we allow a secondary not enrolled who is suspended temporally when BecomePrimary?（and will
   * be set to inactive s) usually it ok for pss, because even though he is back later, and voting
   * with others, there will be a new P, and it is not him. but for psa, consider the case that the
   * membership is pia after become primary(the i is not dead but only suspend a while and do not
   * response the preP), and then we kill the p, and the i back, the i can not voting with the A to
   * get a new p. this is only a network or server shake, but we treat it as the member down, and it
   * will increase the risk of unavailable service. same for pssaa. so this method tell the one who
   * is become primary, to carefully deal with the suspend secondary who is not reachable to you,
   * but may be he is reachable with others. only when the suspend secondary is not reachable to
   * quorum, you can put it into inactive secondary.
   */
  public boolean safeToBecomePrimaryWithAsecondaryMissing() {
    return true;
  }
}