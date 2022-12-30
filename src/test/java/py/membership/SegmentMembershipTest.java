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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;
import py.archive.segment.SegmentVersion;
import py.common.RequestIdBuilder;
import py.instance.InstanceId;
import py.test.TestBase;
import py.volume.VolumeType;

public class SegmentMembershipTest extends TestBase {
  private InstanceId primaryNew = new InstanceId();
  private List<InstanceId> secondariesNew = new ArrayList<InstanceId>();
  private List<InstanceId> arbitersNew = new ArrayList<InstanceId>();
  private List<InstanceId> inactiveSecondariesNew = new ArrayList<InstanceId>();
  private List<InstanceId> joiningSecondariesNew = new ArrayList<InstanceId>();
  private Long instanceId = 0L;

  @Test
  public void testCompareTo() throws Exception {
    SegmentMembership membershipOld = new SegmentMembership(new SegmentVersion(0, 0),
        new InstanceId(0L),
        new ArrayList<InstanceId>() {
          {
            add(new InstanceId(1L));
            add(new InstanceId(2L));
          }
        });
    SegmentMembership membershipNew = new SegmentMembership(new SegmentVersion(1, 0),
        new InstanceId(0L),
        new ArrayList<InstanceId>() {
          {
            add(new InstanceId(1L));
            add(new InstanceId(2L));
          }
        });
    assertTrue(membershipNew.compareTo(membershipOld) > 0);

    serializeTest(membershipNew);
    serializeTest(membershipOld);
  }

  @Test
  public void testEquals() throws Exception {
    SegmentMembership membershipOld = new SegmentMembership(new SegmentVersion(0, 0),
        new InstanceId(0L),
        new ArrayList<InstanceId>() {
          {
            add(new InstanceId(1L));
            add(new InstanceId(2L));
          }
        },
        new ArrayList<InstanceId>() {
          {
            add(new InstanceId(4L));
            add(new InstanceId(5L));
          }
        },
        new ArrayList<InstanceId>() {
          {
            add(new InstanceId(6L));
            add(new InstanceId(7L));
          }
        },
        new ArrayList<InstanceId>() {
          {
            add(new InstanceId(8L));
            add(new InstanceId(9L));
          }
        });

    SegmentMembership membershipNew = new SegmentMembership(new SegmentVersion(0, 0),
        new InstanceId(0L),
        new ArrayList<InstanceId>() {
          {
            add(new InstanceId(1L));
            add(new InstanceId(2L));
          }
        },
        new ArrayList<InstanceId>() {
          {
            add(new InstanceId(4L));
            add(new InstanceId(5L));
          }
        },
        new ArrayList<InstanceId>() {
          {
            add(new InstanceId(6L));
            add(new InstanceId(7L));
          }
        },
        new ArrayList<InstanceId>() {
          {
            add(new InstanceId(8L));
            add(new InstanceId(9L));
          }
        });

    assertTrue(membershipNew.equals(membershipOld));

    SegmentMembership new2 = membershipNew.aliveSecondaryBecomeInactive(new InstanceId(1L));
    assertTrue(!new2.equals(membershipNew));
    assertTrue(!new2.equals(membershipOld));

    serializeTest(membershipNew);
    serializeTest(membershipOld);
  }

  @Test
  public void testSize() throws Exception {
    SegmentMembership membershipOld = new SegmentMembership(new SegmentVersion(0, 0),
        new InstanceId(0L),
        new ArrayList<InstanceId>() {
          {
            add(new InstanceId(1L));
            add(new InstanceId(2L));
          }
        },
        new ArrayList<InstanceId>() {
          {
            add(new InstanceId(4L));
            add(new InstanceId(5L));
          }
        },
        new ArrayList<InstanceId>() {
          {
            add(new InstanceId(6L));
            add(new InstanceId(7L));
          }
        },
        new ArrayList<InstanceId>() {
          {
            add(new InstanceId(8L));
            add(new InstanceId(9L));
          }
        });

    assertEquals(9, membershipOld.size());

    serializeTest(membershipOld);
  }

  @Test
  public void testChangeSecondaries() throws Exception {
    SegmentMembership membershipOld = new SegmentMembership(new SegmentVersion(0, 0),
        new InstanceId(0L),
        new ArrayList<InstanceId>() {
          {
            add(new InstanceId(1L));
            add(new InstanceId(2L));
          }
        },
        new ArrayList<InstanceId>() {
          {
            add(new InstanceId(4L));
            add(new InstanceId(5L));
          }
        },
        new ArrayList<InstanceId>() {
          {
            add(new InstanceId(6L));
            add(new InstanceId(7L));
          }
        },
        new ArrayList<InstanceId>() {
          {
            add(new InstanceId(8L));
            add(new InstanceId(9L));
          }
        });
    SegmentMembership newMembership = membershipOld.addSecondaries(new InstanceId(1L));
    assertEquals(9, newMembership.size());
    assertEquals(membershipOld.getSegmentVersion().getGeneration(),
        newMembership.getSegmentVersion().getGeneration());

    newMembership = membershipOld.addSecondaries(new InstanceId(9L));
    assertEquals(10, newMembership.size());
    assertEquals(membershipOld.getSegmentVersion().getGeneration() + 1,
        newMembership.getSegmentVersion().getGeneration());

    newMembership = membershipOld
        .replaceSecondary(new InstanceId(1L), new InstanceId(new InstanceId(5L)));
    assertEquals(9, newMembership.size());
    assertEquals(membershipOld.getSegmentVersion().getGeneration() + 1,
        newMembership.getSegmentVersion().getGeneration());

    newMembership = membershipOld
        .replaceSecondary(new InstanceId(3L), new InstanceId(new InstanceId(4L)));
    assertNull(newMembership);

    newMembership = membershipOld.removeSecondary(new InstanceId(1L));
    assertEquals(8, newMembership.size());
    assertEquals(membershipOld.getSegmentVersion().getGeneration() + 1,
        newMembership.getSegmentVersion().getGeneration());

    logger.info(newMembership.toString());

    serializeTest(newMembership);
    serializeTest(membershipOld);
  }

  @Test
  public void testAliveSecondaryBecomeInactive() throws Exception {
    SegmentMembership membership = new SegmentMembership(new SegmentVersion(0, 0),
        new InstanceId(0L),
        new ArrayList<InstanceId>() {
          {
            add(new InstanceId(1L));
            add(new InstanceId(2L));
          }
        });

    SegmentMembership newMembership = membership.aliveSecondaryBecomeInactive(new InstanceId(1L));
    assertTrue(newMembership.getInactiveSecondaries().contains(new InstanceId(1L)));
    assertTrue(!newMembership.getSecondaries().contains(new InstanceId(1L)));
    assertEquals(membership.getSegmentVersion().getGeneration() + 1,
        newMembership.getSegmentVersion().getGeneration());

    SegmentMembership membershipNotChanged = membership
        .aliveSecondaryBecomeInactive(new InstanceId(6L));
    assertEquals(membership.getSegmentVersion().getGeneration(),
        membershipNotChanged.getSegmentVersion().getGeneration());

    // change the inactive one to the joining one to prepare for the test
    newMembership = newMembership.inactiveSecondaryBecomeJoining(new InstanceId(1L));
    // change the joining one back to the inactive one
    newMembership = newMembership.aliveSecondaryBecomeInactive(new InstanceId(1L));
    assertEquals(membership.getSegmentVersion().getGeneration() + 3,
        newMembership.getSegmentVersion().getGeneration());
    assertTrue(newMembership.getInactiveSecondaries().contains(new InstanceId(1L)));
    assertTrue(!newMembership.getSecondaries().contains(new InstanceId(1L)));
    assertTrue(!newMembership.getJoiningSecondaries().contains(new InstanceId(1L)));

    serializeTest(newMembership);
    serializeTest(membership);
  }

  @Test
  public void testRemoveInactiveSecondary() throws Exception {
    SegmentMembership membership = new SegmentMembership(new SegmentVersion(0, 0),
        new InstanceId(0L), null, null,
        new ArrayList<InstanceId>() {
          {
            add(new InstanceId(1L));
            add(new InstanceId(2L));
          }
        }, null);

    SegmentMembership newMembership = membership.removeInactiveSecondary(new InstanceId(1L));
    logger.info(membership.toString());
    logger.info(newMembership.toString());

    assertTrue(!newMembership.getInactiveSecondaries().contains(new InstanceId(1L)));
    assertEquals(2, newMembership.size());
    assertEquals(membership.getSegmentVersion().getGeneration() + 1,
        newMembership.getSegmentVersion().getGeneration());

    newMembership = membership.removeInactiveSecondary(new InstanceId(6L));
    assertEquals(membership.getSegmentVersion().getGeneration(),
        newMembership.getSegmentVersion().getGeneration());

    serializeTest(newMembership);
    serializeTest(membership);
  }

  @Test
  public void testAddJoiningSecondary() throws Exception {
    SegmentMembership membership = new SegmentMembership(new SegmentVersion(0, 0),
        new InstanceId(0L),
        new ArrayList<InstanceId>() {
          {
            add(new InstanceId(1L));
            add(new InstanceId(2L));
          }
        });

    SegmentMembership newMembership = membership.addJoiningSecondary(new InstanceId(3L));
    assertTrue(newMembership.getJoiningSecondaries().contains(new InstanceId(3L)));
    assertEquals(4, newMembership.size());
    assertEquals(membership.getSegmentVersion().getGeneration() + 1,
        newMembership.getSegmentVersion().getGeneration());

    SegmentMembership newMembership2 = newMembership.addJoiningSecondary(new InstanceId(3L));
    assertEquals(4, newMembership.size());
    assertEquals(newMembership.getSegmentVersion().getGeneration(),
        newMembership2.getSegmentVersion().getGeneration());
    serializeTest(newMembership);
    serializeTest(membership);
    serializeTest(newMembership2);
  }

  @Test
  public void testJoiningSecondaryBecomeSecondary() throws Exception {
    SegmentMembership membership = new SegmentMembership(new SegmentVersion(0, 0),
        new InstanceId(0L),
        new ArrayList<InstanceId>() {
          {
            add(new InstanceId(1L));
            add(new InstanceId(2L));
          }
        });

    SegmentMembership newMembership = membership.addJoiningSecondary(new InstanceId(3L));
    SegmentMembership newMembership2 = newMembership
        .joiningSecondaryBecomeSecondary(new InstanceId(3L));
    logger.info(membership.toString());
    logger.info(newMembership.toString());
    logger.info(newMembership2.toString());
    assertTrue(!newMembership2.getJoiningSecondaries().contains(new InstanceId(3L)));
    assertEquals(membership.getSegmentVersion().getGeneration() + 1,
        newMembership.getSegmentVersion().getGeneration());
    assertTrue(newMembership2.getSecondaries().contains(new InstanceId(3L)));
    assertEquals(4, newMembership2.size());
    assertEquals(membership.getSegmentVersion().getGeneration() + 2,
        newMembership2.getSegmentVersion().getGeneration());

    SegmentMembership newMembership3 = newMembership
        .joiningSecondaryBecomeSecondary(new InstanceId(6L));
    assertEquals(newMembership.getSegmentVersion().getGeneration(),
        newMembership3.getSegmentVersion().getGeneration());

    serializeTest(newMembership);
    serializeTest(membership);
    serializeTest(newMembership2);
    serializeTest(newMembership3);
  }

  @Test
  public void joiningSecondaryBecomeInactive() throws Exception {
    SegmentMembership membership = new SegmentMembership(new SegmentVersion(0, 0),
        new InstanceId(0L),
        new ArrayList<InstanceId>() {
          {
            add(new InstanceId(1L));
            add(new InstanceId(2L));
          }
        });

    SegmentMembership newMembership = membership.addJoiningSecondary(new InstanceId(3L));
    SegmentMembership newMembership2 = newMembership
        .joiningSecondaryBecomeInactive(new InstanceId(3L));
    logger.info(membership.toString());
    logger.info(newMembership.toString());
    logger.info(newMembership2.toString());
    assertTrue(!newMembership2.getJoiningSecondaries().contains(new InstanceId(3L)));
    assertEquals(membership.getSegmentVersion().getGeneration() + 1,
        newMembership.getSegmentVersion().getGeneration());
    assertTrue(newMembership2.getInactiveSecondaries().contains(new InstanceId(3L)));
    assertEquals(4, newMembership2.size());
    assertEquals(newMembership.getSegmentVersion().getGeneration() + 1,
        newMembership2.getSegmentVersion().getGeneration());

    SegmentMembership newMembership3 = newMembership
        .joiningSecondaryBecomeInactive(new InstanceId(6L));
    assertEquals(newMembership3.getSegmentVersion().getGeneration(),
        newMembership.getSegmentVersion().getGeneration());
    serializeTest(newMembership);
    serializeTest(membership);
    serializeTest(newMembership2);
    serializeTest(newMembership3);
  }

  @Test
  public void testJson() throws Exception {
    SegmentMembership membershipOld = new SegmentMembership(new SegmentVersion(0, 0),
        new InstanceId(0L),
        new ArrayList<InstanceId>() {
          {
            add(new InstanceId(1L));
            add(new InstanceId(2L));
          }
        },
        new ArrayList<InstanceId>() {
          {
            add(new InstanceId(4L));
            add(new InstanceId(5L));
          }
        },
        new ArrayList<InstanceId>() {
          {
            add(new InstanceId(6L));
            add(new InstanceId(7L));
          }
        },
        new ArrayList<InstanceId>() {
          {
            add(new InstanceId(8L));
            add(new InstanceId(9L));
          }
        });

    ObjectMapper mapper = new ObjectMapper();
    String membershipString = mapper.writeValueAsString(membershipOld);
    logger.info(membershipString);
    SegmentMembership newMembership = mapper.readValue(membershipString, SegmentMembership.class);
    assertEquals(newMembership, membershipOld);

    membershipOld = new SegmentMembership(new SegmentVersion(0, 0), new InstanceId(0L),
        new ArrayList<InstanceId>() {
          {
            add(new InstanceId(1L));
            add(new InstanceId(2L));
          }
        });
    membershipString = mapper.writeValueAsString(membershipOld);
    logger.info(membershipString);
    newMembership = mapper.readValue(membershipString, SegmentMembership.class);
    assertEquals(newMembership, membershipOld);

    serializeTest(newMembership);
    serializeTest(membershipOld);
  }

  @Test
  public void testNewMembershipReferenceCanBeReferenced() throws Exception {
    SegmentMembership membershipOld = new SegmentMembership(new SegmentVersion(0, 0),
        new InstanceId(0L),
        new ArrayList<InstanceId>() {
          {
            add(new InstanceId(1L));
            add(new InstanceId(2L));
          }
        },
        new ArrayList<InstanceId>() {
          {
            add(new InstanceId(4L));
            add(new InstanceId(5L));
          }
        },
        new ArrayList<InstanceId>() {
          {
            add(new InstanceId(6L));
            add(new InstanceId(7L));
          }
        },
        new ArrayList<InstanceId>() {
          {
            add(new InstanceId(8L));
            add(new InstanceId(9L));
          }
        });

    SegmentMembership newMembership = membershipOld;

    newMembership = newMembership.aliveSecondaryBecomeInactive(new InstanceId(1L));
    assertEquals(9, newMembership.size());
    assertEquals(membershipOld.getSegmentVersion().getGeneration() + 1,
        newMembership.getSegmentVersion().getGeneration());
    assertEquals(3, newMembership.getInactiveSecondaries().size());
    logger.info(newMembership.toString());

    serializeTest(newMembership);
    serializeTest(membershipOld);
  }

  @Test
  public void testWriteResultOfPa() throws Exception {
    Long id = 0L;
    InstanceId primary = new InstanceId(id++);
    List<InstanceId> secondaries = new ArrayList<InstanceId>();
    List<InstanceId> arbiters = new ArrayList<InstanceId>();
    arbiters.add(new InstanceId(id++));
    List<InstanceId> inactiveSecondaries = new ArrayList<InstanceId>();
    List<InstanceId> joiningSecondaries = new ArrayList<InstanceId>();
    SegmentMembership membership = new SegmentMembership(new SegmentVersion(0, 0), primary,
        secondaries, arbiters,
        inactiveSecondaries, joiningSecondaries);
    int nwriteQuorum = 2;
    int numArbitersCount = 1;
    int numSecondariesCount = 0;
    int numJoiningSecondariesCount = 0;
    boolean writeResult = membership
        .checkWriteResultOfSecondariesAndArbiters(nwriteQuorum, numSecondariesCount,
            numJoiningSecondariesCount, numArbitersCount);
    assertTrue(writeResult);

    serializeTest(membership);
  }

  @Test
  public void testWriteResultOfPsa() throws Exception {
    Long id = 0L;
    InstanceId primary = new InstanceId(id++);
    List<InstanceId> secondaries = new ArrayList<InstanceId>();
    secondaries.add(new InstanceId(id++));
    List<InstanceId> arbiters = new ArrayList<InstanceId>();
    arbiters.add(new InstanceId(id++));
    List<InstanceId> inactiveSecondaries = new ArrayList<InstanceId>();
    List<InstanceId> joiningSecondaries = new ArrayList<InstanceId>();
    SegmentMembership membership = new SegmentMembership(new SegmentVersion(0, 0), primary,
        secondaries, arbiters,
        inactiveSecondaries, joiningSecondaries);
    int nwriteQuorum = 2;
    int numSecondariesCount = 1;
    int numJoiningSecondariesCount = 0;
    int numArbitersCount = 0;

    boolean writeResult = membership
        .checkWriteResultOfSecondariesAndArbiters(nwriteQuorum, numSecondariesCount,
            numJoiningSecondariesCount, numArbitersCount);
    assertTrue(writeResult);

    serializeTest(membership);
  }

  @Test
  public void testWriteResultOfPja() throws Exception {
    Long id = 0L;
    InstanceId primary = new InstanceId(id++);
    List<InstanceId> secondaries = new ArrayList<InstanceId>();
    List<InstanceId> arbiters = new ArrayList<InstanceId>();
    arbiters.add(new InstanceId(id++));
    List<InstanceId> inactiveSecondaries = new ArrayList<InstanceId>();
    List<InstanceId> joiningSecondaries = new ArrayList<InstanceId>();
    joiningSecondaries.add(new InstanceId(id++));
    SegmentMembership membership = new SegmentMembership(new SegmentVersion(0, 0), primary,
        secondaries, arbiters,
        inactiveSecondaries, joiningSecondaries);
    int nwriteQuorum = 2;
    int numSecondariesCount = 0;
    int numJoiningSecondariesCount = 1;
    int numArbitersCount = 0;

    boolean writeResult = membership
        .checkWriteResultOfSecondariesAndArbiters(nwriteQuorum, numSecondariesCount,
            numJoiningSecondariesCount, numArbitersCount);
    assertTrue(writeResult);

    serializeTest(membership);
  }

  @Test
  public void testWriteResultOfPja2() throws Exception {
    Long id = 0L;
    InstanceId primary = new InstanceId(id++);
    List<InstanceId> secondaries = new ArrayList<InstanceId>();
    List<InstanceId> arbiters = new ArrayList<InstanceId>();
    arbiters.add(new InstanceId(id++));
    List<InstanceId> inactiveSecondaries = new ArrayList<InstanceId>();
    List<InstanceId> joiningSecondaries = new ArrayList<InstanceId>();
    joiningSecondaries.add(new InstanceId(id++));
    SegmentMembership membership = new SegmentMembership(new SegmentVersion(0, 0), primary,
        secondaries, arbiters,
        inactiveSecondaries, joiningSecondaries);
    int nwriteQuorum = 2;
    int numSecondariesCount = 0;
    int numJoiningSecondariesCount = 0;
    int numArbitersCount = 1;

    boolean writeResult = membership
        .checkWriteResultOfSecondariesAndArbiters(nwriteQuorum, numSecondariesCount,
            numJoiningSecondariesCount, numArbitersCount);
    assertTrue(!writeResult);

    serializeTest(membership);
  }

  @Test
  public void testWriteResultOfPss() throws Exception {
    Long id = 0L;
    InstanceId primary = new InstanceId(id++);
    List<InstanceId> secondaries = new ArrayList<InstanceId>();
    secondaries.add(new InstanceId(id++));
    secondaries.add(new InstanceId(id++));
    List<InstanceId> arbiters = new ArrayList<InstanceId>();
    List<InstanceId> inactiveSecondaries = new ArrayList<InstanceId>();
    List<InstanceId> joiningSecondaries = new ArrayList<InstanceId>();
    SegmentMembership membership = new SegmentMembership(new SegmentVersion(0, 0), primary,
        secondaries, arbiters,
        inactiveSecondaries, joiningSecondaries);
    int nwriteQuorum = 2;
    int numSecondariesCount = 1;
    int numJoiningSecondariesCount = 0;
    int numArbitersCount = 0;
    boolean writeResult = membership
        .checkWriteResultOfSecondariesAndArbiters(nwriteQuorum, numSecondariesCount,
            numJoiningSecondariesCount, numArbitersCount);
    assertTrue(writeResult);

    serializeTest(membership);
  }

  /**
   * should write to P+S.
   */
  @Test
  public void testWriteResultOfPsj() throws Exception {
    Long id = 0L;
    InstanceId primary = new InstanceId(id++);
    List<InstanceId> secondaries = new ArrayList<InstanceId>();
    secondaries.add(new InstanceId(id++));
    List<InstanceId> arbiters = new ArrayList<InstanceId>();
    List<InstanceId> inactiveSecondaries = new ArrayList<InstanceId>();
    List<InstanceId> joiningSecondaries = new ArrayList<InstanceId>();
    joiningSecondaries.add(new InstanceId(id++));
    SegmentMembership membership = new SegmentMembership(new SegmentVersion(0, 0), primary,
        secondaries, arbiters,
        inactiveSecondaries, joiningSecondaries);
    int nwriteQuorum = 2;
    int numSecondariesCount = 1;
    int numJoiningSecondariesCount = 0;
    int numArbitersCount = 0;
    boolean writeResult = membership
        .checkWriteResultOfSecondariesAndArbiters(nwriteQuorum, numSecondariesCount,
            numJoiningSecondariesCount, numArbitersCount);
    assertTrue(writeResult);

    serializeTest(membership);
  }

  /**
   * not write to P+S, but P+J.
   */
  @Test
  public void testWriteResultOfPsj1() throws Exception {
    Long id = 0L;
    InstanceId primary = new InstanceId(id++);
    List<InstanceId> secondaries = new ArrayList<InstanceId>();
    secondaries.add(new InstanceId(id++));
    List<InstanceId> arbiters = new ArrayList<InstanceId>();
    List<InstanceId> inactiveSecondaries = new ArrayList<InstanceId>();
    List<InstanceId> joiningSecondaries = new ArrayList<InstanceId>();
    joiningSecondaries.add(new InstanceId(id++));
    SegmentMembership membership = new SegmentMembership(new SegmentVersion(0, 0), primary,
        secondaries, arbiters,
        inactiveSecondaries, joiningSecondaries);
    int nwriteQuorum = 2;
    int numSecondariesCount = 0;
    int numJoiningSecondariesCount = 1;
    int numArbitersCount = 0;
    boolean writeResult = membership
        .checkWriteResultOfSecondariesAndArbiters(nwriteQuorum, numSecondariesCount,
            numJoiningSecondariesCount, numArbitersCount);
    assertTrue(!writeResult);

    serializeTest(membership);
  }

  @Test
  public void testWriteResultOfPs() throws Exception {
    Long id = 0L;
    InstanceId primary = new InstanceId(id++);
    List<InstanceId> secondaries = new ArrayList<InstanceId>();
    secondaries.add(new InstanceId(id++));
    List<InstanceId> arbiters = new ArrayList<InstanceId>();
    List<InstanceId> inactiveSecondaries = new ArrayList<InstanceId>();
    List<InstanceId> joiningSecondaries = new ArrayList<InstanceId>();
    SegmentMembership membership = new SegmentMembership(new SegmentVersion(0, 0), primary,
        secondaries, arbiters,
        inactiveSecondaries, joiningSecondaries);
    int nwriteQuorum = 2;
    int numSecondariesCount = 1;
    int numJoiningSecondariesCount = 0;
    int numArbitersCount = 0;
    boolean writeResult = membership
        .checkWriteResultOfSecondariesAndArbiters(nwriteQuorum, numSecondariesCount,
            numJoiningSecondariesCount, numArbitersCount);
    assertTrue(writeResult);

    serializeTest(membership);
  }

  @Test
  public void testWriteResultOfPj() throws Exception {
    Long id = 0L;
    InstanceId primary = new InstanceId(id++);
    List<InstanceId> secondaries = new ArrayList<InstanceId>();
    List<InstanceId> arbiters = new ArrayList<InstanceId>();
    List<InstanceId> inactiveSecondaries = new ArrayList<InstanceId>();
    List<InstanceId> joiningSecondaries = new ArrayList<InstanceId>();
    joiningSecondaries.add(new InstanceId(id++));
    SegmentMembership membership = new SegmentMembership(new SegmentVersion(0, 0), primary,
        secondaries, arbiters,
        inactiveSecondaries, joiningSecondaries);
    int nwriteQuorum = 2;
    int numSecondariesCount = 0;
    int numJoiningSecondariesCount = 1;
    int numArbitersCount = 0;
    boolean writeResult = membership
        .checkWriteResultOfSecondariesAndArbiters(nwriteQuorum, numSecondariesCount,
            numJoiningSecondariesCount, numArbitersCount);
    assertTrue(writeResult);

    serializeTest(membership);
  }

  /**
   * split check write secondaries count and check good write secondaries count large volume.
   */
  @Test
  public void testWriteResultOfPssaa() throws Exception {
    Long id = 0L;
    final InstanceId primary = new InstanceId(id++);
    List<InstanceId> secondaries = new ArrayList<InstanceId>();
    List<InstanceId> arbiters = new ArrayList<InstanceId>();
    final List<InstanceId> inactiveSecondaries = new ArrayList<InstanceId>();
    final List<InstanceId> joiningSecondaries = new ArrayList<InstanceId>();

    secondaries.add(new InstanceId(id++));
    secondaries.add(new InstanceId(id++));
    arbiters.add(new InstanceId(id++));
    arbiters.add(new InstanceId(id++));
    SegmentMembership membership = new SegmentMembership(new SegmentVersion(0, 0), primary,
        secondaries, arbiters,
        inactiveSecondaries, joiningSecondaries);
    final int nwriteQuorum = 3;
    int numSecondariesCount = 2;
    int numJoiningSecondariesCount = 0;
    int numArbitersCount = 0;
    boolean writeResult = membership
        .checkWriteResultOfSecondariesAndArbiters(nwriteQuorum, numSecondariesCount,
            numJoiningSecondariesCount, numArbitersCount);
    assertTrue(writeResult);

    serializeTest(membership);
  }

  @Test
  public void testWriteResultOfPssaa1() throws Exception {
    Long id = 0L;
    final InstanceId primary = new InstanceId(id++);
    List<InstanceId> secondaries = new ArrayList<InstanceId>();
    List<InstanceId> arbiters = new ArrayList<InstanceId>();
    final List<InstanceId> inactiveSecondaries = new ArrayList<InstanceId>();
    final List<InstanceId> joiningSecondaries = new ArrayList<InstanceId>();

    secondaries.add(new InstanceId(id++));
    secondaries.add(new InstanceId(id++));
    arbiters.add(new InstanceId(id++));
    arbiters.add(new InstanceId(id++));
    SegmentMembership membership = new SegmentMembership(new SegmentVersion(0, 0), primary,
        secondaries, arbiters,
        inactiveSecondaries, joiningSecondaries);
    final int nwriteQuorum = 3;
    int numSecondariesCount = 1;
    int numJoiningSecondariesCount = 0;
    int numArbitersCount = 2;
    boolean writeResult = membership
        .checkWriteResultOfSecondariesAndArbiters(nwriteQuorum, numSecondariesCount,
            numJoiningSecondariesCount, numArbitersCount);
    assertTrue(!writeResult);

    serializeTest(membership);
  }

  @Test
  public void testBadWriteResultOfPssaa() throws Exception {
    Long id = 0L;
    final InstanceId primary = new InstanceId(id++);
    List<InstanceId> secondaries = new ArrayList<InstanceId>();
    List<InstanceId> arbiters = new ArrayList<InstanceId>();
    final List<InstanceId> inactiveSecondaries = new ArrayList<InstanceId>();
    final List<InstanceId> joiningSecondaries = new ArrayList<InstanceId>();

    secondaries.add(new InstanceId(id++));
    secondaries.add(new InstanceId(id++));
    arbiters.add(new InstanceId(id++));
    arbiters.add(new InstanceId(id++));
    SegmentMembership membership = new SegmentMembership(new SegmentVersion(0, 0), primary,
        secondaries, arbiters,
        inactiveSecondaries, joiningSecondaries);
    final int nwriteQuorum = 3;
    int numBadSecondariesCount = 1;
    int numBadJoiningSecondariesCount = 0;
    int numBadArbitersCount = 0;
    boolean writeResultBad = membership
        .checkBadWriteResultOfSecondariesAndArbiters(nwriteQuorum, numBadSecondariesCount,
            numBadJoiningSecondariesCount, numBadArbitersCount);
    assertTrue(writeResultBad);

    serializeTest(membership);
  }

  @Test
  public void testWriteResultOfPjsaa() throws Exception {
    Long id = 0L;
    final InstanceId primary = new InstanceId(id++);
    List<InstanceId> secondaries = new ArrayList<InstanceId>();
    List<InstanceId> arbiters = new ArrayList<InstanceId>();
    final List<InstanceId> inactiveSecondaries = new ArrayList<InstanceId>();
    List<InstanceId> joiningSecondaries = new ArrayList<InstanceId>();

    secondaries.add(new InstanceId(id++));
    joiningSecondaries.add(new InstanceId(id++));
    arbiters.add(new InstanceId(id++));
    arbiters.add(new InstanceId(id++));
    SegmentMembership membership = new SegmentMembership(new SegmentVersion(0, 0), primary,
        secondaries, arbiters,
        inactiveSecondaries, joiningSecondaries);
    final int nwriteQuorum = 3;
    int numSecondariesCount = 1;
    int numJoiningSecondariesCount = 1;
    int numArbitersCount = 0;
    boolean writeResult = membership
        .checkWriteResultOfSecondariesAndArbiters(nwriteQuorum, numSecondariesCount,
            numJoiningSecondariesCount, numArbitersCount);
    assertTrue(writeResult);

    serializeTest(membership);
  }

  @Test
  public void testWriteResultOfPjsaa1() throws Exception {
    Long id = 0L;
    final InstanceId primary = new InstanceId(id++);
    List<InstanceId> secondaries = new ArrayList<InstanceId>();
    List<InstanceId> arbiters = new ArrayList<InstanceId>();
    final List<InstanceId> inactiveSecondaries = new ArrayList<InstanceId>();
    List<InstanceId> joiningSecondaries = new ArrayList<InstanceId>();

    secondaries.add(new InstanceId(id++));
    joiningSecondaries.add(new InstanceId(id++));
    arbiters.add(new InstanceId(id++));
    arbiters.add(new InstanceId(id++));
    SegmentMembership membership = new SegmentMembership(new SegmentVersion(0, 0), primary,
        secondaries, arbiters,
        inactiveSecondaries, joiningSecondaries);
    final int nwriteQuorum = 3;
    int numSecondariesCount = 0;
    int numJoiningSecondariesCount = 1;
    int numArbitersCount = 2;
    boolean writeResult = membership
        .checkWriteResultOfSecondariesAndArbiters(nwriteQuorum, numSecondariesCount,
            numJoiningSecondariesCount, numArbitersCount);
    assertTrue(!writeResult);

    serializeTest(membership);
  }

  @Test
  public void testWriteResultOfPjsaa2() throws Exception {
    Long id = 0L;
    final InstanceId primary = new InstanceId(id++);
    List<InstanceId> secondaries = new ArrayList<InstanceId>();
    List<InstanceId> arbiters = new ArrayList<InstanceId>();
    final List<InstanceId> inactiveSecondaries = new ArrayList<InstanceId>();
    List<InstanceId> joiningSecondaries = new ArrayList<InstanceId>();

    secondaries.add(new InstanceId(id++));
    joiningSecondaries.add(new InstanceId(id++));
    arbiters.add(new InstanceId(id++));
    arbiters.add(new InstanceId(id++));
    SegmentMembership membership = new SegmentMembership(new SegmentVersion(0, 0), primary,
        secondaries, arbiters,
        inactiveSecondaries, joiningSecondaries);
    final int nwriteQuorum = 3;
    int numSecondariesCount = 1;
    int numJoiningSecondariesCount = 0;
    int numArbitersCount = 2;
    boolean writeResult = membership
        .checkWriteResultOfSecondariesAndArbiters(nwriteQuorum, numSecondariesCount,
            numJoiningSecondariesCount, numArbitersCount);
    assertTrue(!writeResult);

    serializeTest(membership);
  }

  @Test
  public void testBadWriteResultOfPjsaa() throws Exception {
    Long id = 0L;
    final InstanceId primary = new InstanceId(id++);
    List<InstanceId> secondaries = new ArrayList<InstanceId>();
    List<InstanceId> arbiters = new ArrayList<InstanceId>();
    final List<InstanceId> inactiveSecondaries = new ArrayList<InstanceId>();
    List<InstanceId> joiningSecondaries = new ArrayList<InstanceId>();

    secondaries.add(new InstanceId(id++));
    joiningSecondaries.add(new InstanceId(id++));
    arbiters.add(new InstanceId(id++));
    arbiters.add(new InstanceId(id++));
    SegmentMembership membership = new SegmentMembership(new SegmentVersion(0, 0), primary,
        secondaries, arbiters,
        inactiveSecondaries, joiningSecondaries);
    final int nwriteQuorum = 3;
    int numBadSecondariesCount = 1;
    int numBadJoiningSecondariesCount = 0;
    int numBadArbitersCount = 0;
    boolean writeResultBad = membership
        .checkBadWriteResultOfSecondariesAndArbiters(nwriteQuorum, numBadSecondariesCount,
            numBadJoiningSecondariesCount, numBadArbitersCount);
    assertTrue(writeResultBad);

    serializeTest(membership);
  }

  @Test
  public void testBadWriteResultOfPjsaa1() throws Exception {
    Long id = 0L;
    final InstanceId primary = new InstanceId(id++);
    List<InstanceId> secondaries = new ArrayList<InstanceId>();
    List<InstanceId> arbiters = new ArrayList<InstanceId>();
    final List<InstanceId> inactiveSecondaries = new ArrayList<InstanceId>();
    List<InstanceId> joiningSecondaries = new ArrayList<InstanceId>();

    secondaries.add(new InstanceId(id++));
    joiningSecondaries.add(new InstanceId(id++));
    arbiters.add(new InstanceId(id++));
    arbiters.add(new InstanceId(id++));
    SegmentMembership membership = new SegmentMembership(new SegmentVersion(0, 0), primary,
        secondaries, arbiters,
        inactiveSecondaries, joiningSecondaries);
    final int nwriteQuorum = 3;
    int numBadSecondariesCount = 0;
    int numBadJoiningSecondariesCount = 1;
    int numBadArbitersCount = 0;
    boolean writeResultBad = membership
        .checkBadWriteResultOfSecondariesAndArbiters(nwriteQuorum, numBadSecondariesCount,
            numBadJoiningSecondariesCount, numBadArbitersCount);
    assertTrue(writeResultBad);

    serializeTest(membership);
  }

  @Test
  public void testBadWriteResultOfPjsaa2() throws Exception {
    Long id = 0L;
    final InstanceId primary = new InstanceId(id++);
    List<InstanceId> secondaries = new ArrayList<InstanceId>();
    List<InstanceId> arbiters = new ArrayList<InstanceId>();
    final List<InstanceId> inactiveSecondaries = new ArrayList<InstanceId>();
    List<InstanceId> joiningSecondaries = new ArrayList<InstanceId>();

    secondaries.add(new InstanceId(id++));
    joiningSecondaries.add(new InstanceId(id++));
    arbiters.add(new InstanceId(id++));
    arbiters.add(new InstanceId(id++));
    SegmentMembership membership = new SegmentMembership(new SegmentVersion(0, 0), primary,
        secondaries, arbiters,
        inactiveSecondaries, joiningSecondaries);
    final int nwriteQuorum = 3;
    int numBadSecondariesCount = 0;
    int numBadJoiningSecondariesCount = 0;
    int numBadArbitersCount = 2;
    boolean writeResultBad = membership
        .checkBadWriteResultOfSecondariesAndArbiters(nwriteQuorum, numBadSecondariesCount,
            numBadJoiningSecondariesCount, numBadArbitersCount);
    assertTrue(!writeResultBad);

    serializeTest(membership);
  }

  @Test
  public void testWriteResultOfPsaa() throws Exception {
    Long id = 0L;
    final InstanceId primary = new InstanceId(id++);
    List<InstanceId> secondaries = new ArrayList<InstanceId>();
    List<InstanceId> arbiters = new ArrayList<InstanceId>();
    final List<InstanceId> inactiveSecondaries = new ArrayList<InstanceId>();
    final List<InstanceId> joiningSecondaries = new ArrayList<InstanceId>();

    secondaries.add(new InstanceId(id++));
    arbiters.add(new InstanceId(id++));
    arbiters.add(new InstanceId(id++));
    SegmentMembership membership = new SegmentMembership(new SegmentVersion(0, 0), primary,
        secondaries, arbiters,
        inactiveSecondaries, joiningSecondaries);
    final int nwriteQuorum = 3;
    int numSecondariesCount = 1;
    int numJoiningSecondariesCount = 0;
    int numArbitersCount = 0;
    boolean writeResult = membership
        .checkWriteResultOfSecondariesAndArbiters(nwriteQuorum, numSecondariesCount,
            numJoiningSecondariesCount, numArbitersCount);
    assertTrue(!writeResult);

    serializeTest(membership);
  }

  @Test
  public void testWriteResultOfPsaa1() throws Exception {
    Long id = 0L;
    final InstanceId primary = new InstanceId(id++);
    List<InstanceId> secondaries = new ArrayList<InstanceId>();
    List<InstanceId> arbiters = new ArrayList<InstanceId>();
    final List<InstanceId> inactiveSecondaries = new ArrayList<InstanceId>();
    final List<InstanceId> joiningSecondaries = new ArrayList<InstanceId>();

    secondaries.add(new InstanceId(id++));
    arbiters.add(new InstanceId(id++));
    arbiters.add(new InstanceId(id++));
    SegmentMembership membership = new SegmentMembership(new SegmentVersion(0, 0), primary,
        secondaries, arbiters,
        inactiveSecondaries, joiningSecondaries);
    final int nwriteQuorum = 3;
    int numSecondariesCount = 1;
    int numJoiningSecondariesCount = 0;
    int numArbitersCount = 1;
    boolean writeResult = membership
        .checkWriteResultOfSecondariesAndArbiters(nwriteQuorum, numSecondariesCount,
            numJoiningSecondariesCount, numArbitersCount);
    assertTrue(writeResult);

    serializeTest(membership);
  }

  @Test
  public void testWriteResultOfPsaa2() throws Exception {
    Long id = 0L;
    final InstanceId primary = new InstanceId(id++);
    List<InstanceId> secondaries = new ArrayList<InstanceId>();
    List<InstanceId> arbiters = new ArrayList<InstanceId>();
    final List<InstanceId> inactiveSecondaries = new ArrayList<InstanceId>();
    final List<InstanceId> joiningSecondaries = new ArrayList<InstanceId>();

    secondaries.add(new InstanceId(id++));
    arbiters.add(new InstanceId(id++));
    arbiters.add(new InstanceId(id++));
    SegmentMembership membership = new SegmentMembership(new SegmentVersion(0, 0), primary,
        secondaries, arbiters,
        inactiveSecondaries, joiningSecondaries);
    final int nwriteQuorum = 3;
    int numSecondariesCount = 0;
    int numJoiningSecondariesCount = 0;
    int numArbitersCount = 2;
    boolean writeResult = membership
        .checkWriteResultOfSecondariesAndArbiters(nwriteQuorum, numSecondariesCount,
            numJoiningSecondariesCount, numArbitersCount);
    assertTrue(!writeResult);

    serializeTest(membership);
  }

  @Test
  public void testBadWriteResultOfPsaA() throws Exception {
    Long id = 0L;
    final InstanceId primary = new InstanceId(id++);
    List<InstanceId> secondaries = new ArrayList<InstanceId>();
    List<InstanceId> arbiters = new ArrayList<InstanceId>();
    final List<InstanceId> inactiveSecondaries = new ArrayList<InstanceId>();
    final List<InstanceId> joiningSecondaries = new ArrayList<InstanceId>();

    secondaries.add(new InstanceId(id++));
    arbiters.add(new InstanceId(id++));
    arbiters.add(new InstanceId(id++));
    SegmentMembership membership = new SegmentMembership(new SegmentVersion(0, 0), primary,
        secondaries, arbiters,
        inactiveSecondaries, joiningSecondaries);
    final int nwriteQuorum = 3;
    int numBadSecondariesCount = 1;
    int numBadJoiningSecondariesCount = 0;
    int numBadArbitersCount = 0;
    boolean writeResultBad = membership
        .checkBadWriteResultOfSecondariesAndArbiters(nwriteQuorum, numBadSecondariesCount,
            numBadJoiningSecondariesCount, numBadArbitersCount);
    assertTrue(writeResultBad);

    serializeTest(membership);
  }

  @Test
  public void testBadWriteResultOfPsaA1() throws Exception {
    Long id = 0L;
    final InstanceId primary = new InstanceId(id++);
    List<InstanceId> secondaries = new ArrayList<InstanceId>();
    List<InstanceId> arbiters = new ArrayList<InstanceId>();
    final List<InstanceId> inactiveSecondaries = new ArrayList<InstanceId>();
    final List<InstanceId> joiningSecondaries = new ArrayList<InstanceId>();

    secondaries.add(new InstanceId(id++));
    arbiters.add(new InstanceId(id++));
    arbiters.add(new InstanceId(id++));
    SegmentMembership membership = new SegmentMembership(new SegmentVersion(0, 0), primary,
        secondaries, arbiters,
        inactiveSecondaries, joiningSecondaries);
    final int nwriteQuorum = 3;
    int numBadSecondariesCount = 0;
    int numBadJoiningSecondariesCount = 0;
    int numBadArbitersCount = 2;
    boolean writeResultBad = membership
        .checkBadWriteResultOfSecondariesAndArbiters(nwriteQuorum, numBadSecondariesCount,
            numBadJoiningSecondariesCount, numBadArbitersCount);
    assertTrue(writeResultBad);

    serializeTest(membership);
  }

  @Test
  public void testBadWriteResultOfPsaa2() throws Exception {
    Long id = 0L;
    final InstanceId primary = new InstanceId(id++);
    List<InstanceId> secondaries = new ArrayList<InstanceId>();
    secondaries.add(new InstanceId(id++));
    List<InstanceId> arbiters = new ArrayList<InstanceId>();
    arbiters.add(new InstanceId(id++));
    arbiters.add(new InstanceId(id++));
    List<InstanceId> inactiveSecondaries = new ArrayList<InstanceId>();
    List<InstanceId> joiningSecondaries = new ArrayList<InstanceId>();
    SegmentMembership membership = new SegmentMembership(new SegmentVersion(0, 0), primary,
        secondaries, arbiters,
        inactiveSecondaries, joiningSecondaries);
    final int nwriteQuorum = 3;
    int numBadSecondariesCount = 0;
    int numBadJoiningSecondariesCount = 0;
    int numBadArbitersCount = 1;
    boolean writeResultBad = membership
        .checkBadWriteResultOfSecondariesAndArbiters(nwriteQuorum, numBadSecondariesCount,
            numBadJoiningSecondariesCount, numBadArbitersCount);
    assertTrue(!writeResultBad);

    serializeTest(membership);
  }

  @Test
  public void testWriteResultOfPsaLargeVolume() throws Exception {
    Long id = 0L;
    InstanceId primary = new InstanceId(id++);
    List<InstanceId> secondaries = new ArrayList<InstanceId>();
    List<InstanceId> arbiters = new ArrayList<InstanceId>();
    List<InstanceId> inactiveSecondaries = new ArrayList<InstanceId>();
    List<InstanceId> joiningSecondaries = new ArrayList<InstanceId>();

    secondaries.add(new InstanceId(id++));
    arbiters.add(new InstanceId(id++));
    SegmentMembership membership = new SegmentMembership(new SegmentVersion(0, 0), primary,
        secondaries, arbiters,
        inactiveSecondaries, joiningSecondaries);
    final int nwriteQuorum = 3;
    int numSecondariesCount = 1;
    int numJoiningSecondariesCount = 0;
    int numArbitersCount = 0;
    boolean writeResult = membership
        .checkWriteResultOfSecondariesAndArbiters(nwriteQuorum, numSecondariesCount,
            numJoiningSecondariesCount, numArbitersCount);
    assertTrue(!writeResult);

    serializeTest(membership);
  }

  @Test
  public void testWriteResultOfPsaLargeVolume1() throws Exception {
    Long id = 0L;
    InstanceId primary = new InstanceId(id++);
    List<InstanceId> secondaries = new ArrayList<InstanceId>();
    List<InstanceId> arbiters = new ArrayList<InstanceId>();
    List<InstanceId> inactiveSecondaries = new ArrayList<InstanceId>();
    List<InstanceId> joiningSecondaries = new ArrayList<InstanceId>();

    secondaries.add(new InstanceId(id++));
    arbiters.add(new InstanceId(id++));
    SegmentMembership membership = new SegmentMembership(new SegmentVersion(0, 0), primary,
        secondaries, arbiters,
        inactiveSecondaries, joiningSecondaries);
    final int nwriteQuorum = 3;
    int numSecondariesCount = 1;
    int numJoiningSecondariesCount = 0;
    int numArbitersCount = 1;
    boolean writeResult = membership
        .checkWriteResultOfSecondariesAndArbiters(nwriteQuorum, numSecondariesCount,
            numJoiningSecondariesCount, numArbitersCount);
    assertTrue(writeResult);

    serializeTest(membership);
  }

  @Test
  public void testBadWriteResultOfPsaLargeVolume() throws Exception {
    Long id = 0L;
    InstanceId primary = new InstanceId(id++);
    List<InstanceId> secondaries = new ArrayList<InstanceId>();
    List<InstanceId> arbiters = new ArrayList<InstanceId>();
    List<InstanceId> inactiveSecondaries = new ArrayList<InstanceId>();
    List<InstanceId> joiningSecondaries = new ArrayList<InstanceId>();

    secondaries.add(new InstanceId(id++));
    arbiters.add(new InstanceId(id++));
    SegmentMembership membership = new SegmentMembership(new SegmentVersion(0, 0), primary,
        secondaries, arbiters,
        inactiveSecondaries, joiningSecondaries);
    final int nwriteQuorum = 3;
    int numBadSecondariesCount = 0;
    int numBadJoiningSecondariesCount = 0;
    int numBadArbitersCount = 1;
    boolean writeResultBad = membership
        .checkBadWriteResultOfSecondariesAndArbiters(nwriteQuorum, numBadSecondariesCount,
            numBadJoiningSecondariesCount, numBadArbitersCount);
    assertTrue(writeResultBad);

    serializeTest(membership);
  }

  @Test
  public void testArbiterBecomeInactiveWhenTempPrimaryExistOfPssaa() throws Exception {
    SegmentMembership membership = new SegmentMembership(new SegmentVersion(0, 0),
        new InstanceId(0L),
        new ArrayList<InstanceId>() {
          {
            add(new InstanceId(1L));
            add(new InstanceId(2L));
          }
        },
        new ArrayList<InstanceId>() {
          {
            add(new InstanceId(3L));
            add(new InstanceId(4L));
          }
        });

    SegmentMembership newMembership = membership.secondaryBecomeTempPrimary(new InstanceId(1L))
        .arbiterBecomeInactive(new InstanceId(3L));
    assertTrue(newMembership.getInactiveSecondaries().contains(new InstanceId(3L)));
    assertTrue(newMembership.getTempPrimary().equals(new InstanceId(1L)));

    serializeTest(newMembership);
    serializeTest(membership);
  }

  @Test
  public void testPotentialPrimarySelectedWhenTempPrimaryExistOfPssaa() throws Exception {
    SegmentMembership membership = new SegmentMembership(new SegmentVersion(0, 0),
        new InstanceId(0L),
        new ArrayList<InstanceId>() {
          {
            add(new InstanceId(1L));
            add(new InstanceId(2L));
          }
        },
        new ArrayList<InstanceId>() {
          {
            add(new InstanceId(3L));
            add(new InstanceId(4L));
          }
        });

    SegmentMembership newMembership = membership.secondaryBecomeTempPrimary(new InstanceId(1L));
    SegmentMembership newMembership2 = membership.potentialPrimarySelected(new InstanceId(2L));
    assertTrue(newMembership.getSegmentVersion().compareTo(newMembership2.getSegmentVersion()) < 0);

    serializeTest(newMembership);
    serializeTest(membership);
  }

  @Test
  public void testWriteResultOfPjaa() throws Exception {
    Long id = 0L;
    final InstanceId primary = new InstanceId(id++);
    final List<InstanceId> secondaries = new ArrayList<InstanceId>();
    List<InstanceId> joiningSecondaries = new ArrayList<InstanceId>();
    joiningSecondaries.add(new InstanceId(id++));
    List<InstanceId> arbiters = new ArrayList<InstanceId>();
    arbiters.add(new InstanceId(id++));
    arbiters.add(new InstanceId(id++));
    List<InstanceId> inactiveSecondaries = new ArrayList<InstanceId>();
    SegmentMembership membership = new SegmentMembership(new SegmentVersion(0, 0), primary,
        secondaries, arbiters,
        inactiveSecondaries, joiningSecondaries);
    final int nwriteQuorum = 3;
    int numSecondariesCount = 0;
    int numJoiningSecondariesCount = 0;
    int numArbitersCount = 2;
    boolean writeResult = membership
        .checkWriteResultOfSecondariesAndArbiters(nwriteQuorum, numSecondariesCount,
            numJoiningSecondariesCount, numArbitersCount);
    assertTrue(!writeResult);

    serializeTest(membership);
  }

  @Test
  public void testWriteResultOfPjaa1() throws Exception {
    Long id = 0L;
    final InstanceId primary = new InstanceId(id++);
    final List<InstanceId> secondaries = new ArrayList<InstanceId>();
    List<InstanceId> joiningSecondaries = new ArrayList<InstanceId>();
    joiningSecondaries.add(new InstanceId(id++));
    List<InstanceId> arbiters = new ArrayList<InstanceId>();
    arbiters.add(new InstanceId(id++));
    arbiters.add(new InstanceId(id++));
    List<InstanceId> inactiveSecondaries = new ArrayList<InstanceId>();
    SegmentMembership membership = new SegmentMembership(new SegmentVersion(0, 0), primary,
        secondaries, arbiters,
        inactiveSecondaries, joiningSecondaries);
    final int nwriteQuorum = 3;
    int numSecondariesCount = 0;
    int numJoiningSecondariesCount = 1;
    int numArbitersCount = 0;
    boolean writeResult = membership
        .checkWriteResultOfSecondariesAndArbiters(nwriteQuorum, numSecondariesCount,
            numJoiningSecondariesCount, numArbitersCount);
    assertTrue(!writeResult);

    serializeTest(membership);
  }

  @Test
  public void testWriteResultOfPjaa2() throws Exception {
    Long id = 0L;
    final InstanceId primary = new InstanceId(id++);
    List<InstanceId> joiningSecondaries = new ArrayList<InstanceId>();
    joiningSecondaries.add(new InstanceId(id++));
    List<InstanceId> arbiters = new ArrayList<InstanceId>();
    arbiters.add(new InstanceId(id++));
    arbiters.add(new InstanceId(id++));
    List<InstanceId> secondaries = new ArrayList<InstanceId>();
    List<InstanceId> inactiveSecondaries = new ArrayList<InstanceId>();
    SegmentMembership membership = new SegmentMembership(new SegmentVersion(0, 0), primary,
        secondaries, arbiters,
        inactiveSecondaries, joiningSecondaries);
    final int nwriteQuorum = 3;
    int numSecondariesCount = 0;
    int numJoiningSecondariesCount = 1;
    int numArbitersCount = 1;
    boolean writeResult = membership
        .checkWriteResultOfSecondariesAndArbiters(nwriteQuorum, numSecondariesCount,
            numJoiningSecondariesCount, numArbitersCount);
    assertTrue(writeResult);

    serializeTest(membership);
  }

  @Test
  public void testBadWriteResultOfPjaa() throws Exception {
    Long id = 0L;
    final InstanceId primary = new InstanceId(id++);
    List<InstanceId> joiningSecondaries = new ArrayList<InstanceId>();
    joiningSecondaries.add(new InstanceId(id++));
    List<InstanceId> arbiters = new ArrayList<InstanceId>();
    arbiters.add(new InstanceId(id++));
    arbiters.add(new InstanceId(id++));
    List<InstanceId> secondaries = new ArrayList<InstanceId>();
    List<InstanceId> inactiveSecondaries = new ArrayList<InstanceId>();
    SegmentMembership membership = new SegmentMembership(new SegmentVersion(0, 0), primary,
        secondaries, arbiters,
        inactiveSecondaries, joiningSecondaries);
    final int nwriteQuorum = 3;
    int numBadSecondariesCount = 0;
    int numBadJoiningSecondariesCount = 0;
    int numBadArbitersCount = 1;
    boolean writeResultBad = membership
        .checkBadWriteResultOfSecondariesAndArbiters(nwriteQuorum, numBadSecondariesCount,
            numBadJoiningSecondariesCount, numBadArbitersCount);
    assertTrue(!writeResultBad);

    serializeTest(membership);
  }

  @Test
  public void testBadWriteResultOfPjaa1() throws Exception {
    Long id = 0L;
    final InstanceId primary = new InstanceId(id++);
    List<InstanceId> joiningSecondaries = new ArrayList<InstanceId>();
    joiningSecondaries.add(new InstanceId(id++));
    List<InstanceId> arbiters = new ArrayList<InstanceId>();
    arbiters.add(new InstanceId(id++));
    arbiters.add(new InstanceId(id++));
    List<InstanceId> inactiveSecondaries = new ArrayList<InstanceId>();
    List<InstanceId> secondaries = new ArrayList<InstanceId>();
    SegmentMembership membership = new SegmentMembership(new SegmentVersion(0, 0), primary,
        secondaries, arbiters,
        inactiveSecondaries, joiningSecondaries);
    final int nwriteQuorum = 3;
    int numBadSecondariesCount = 0;
    int numBadJoiningSecondariesCount = 1;
    int numBadArbitersCount = 0;
    boolean writeResultBad = membership
        .checkBadWriteResultOfSecondariesAndArbiters(nwriteQuorum, numBadSecondariesCount,
            numBadJoiningSecondariesCount, numBadArbitersCount);
    assertTrue(writeResultBad);

    serializeTest(membership);
  }

  @Test
  public void testBadWriteResultOfPjaa2() throws Exception {
    Long id = 0L;
    List<InstanceId> joiningSecondaries = new ArrayList<InstanceId>();
    joiningSecondaries.add(new InstanceId(id++));
    List<InstanceId> arbiters = new ArrayList<InstanceId>();
    arbiters.add(new InstanceId(id++));
    arbiters.add(new InstanceId(id++));
    List<InstanceId> inactiveSecondaries = new ArrayList<>();
    List<InstanceId> secondaries = new ArrayList<InstanceId>();
    InstanceId primary = new InstanceId(id++);
    SegmentMembership membership = new SegmentMembership(new SegmentVersion(0, 0), primary,
        secondaries, arbiters,
        inactiveSecondaries, joiningSecondaries);
    final int nwriteQuorum = 3;
    int numBadSecondariesCount = 0;
    int numBadJoiningSecondariesCount = 0;
    int numBadArbitersCount = 2;
    boolean writeResultBad = membership
        .checkBadWriteResultOfSecondariesAndArbiters(nwriteQuorum, numBadSecondariesCount,
            numBadJoiningSecondariesCount, numBadArbitersCount);
    assertTrue(writeResultBad);

    serializeTest(membership);
  }

  @Test
  public void testWriteResultOfPjaLargeVolume() throws Exception {
    Long id = 0L;
    InstanceId primary = new InstanceId(id++);
    List<InstanceId> secondaries = new ArrayList<InstanceId>();
    List<InstanceId> arbiters = new ArrayList<InstanceId>();
    List<InstanceId> inactiveSecondaries = new ArrayList<InstanceId>();
    List<InstanceId> joiningSecondaries = new ArrayList<InstanceId>();

    joiningSecondaries.add(new InstanceId(id++));
    arbiters.add(new InstanceId(id++));
    SegmentMembership membership = new SegmentMembership(new SegmentVersion(0, 0), primary,
        secondaries, arbiters,
        inactiveSecondaries, joiningSecondaries);
    final int nwriteQuorum = 3;
    int numSecondariesCount = 0;
    int numJoiningSecondariesCount = 1;
    int numArbitersCount = 0;
    boolean writeResult = membership
        .checkWriteResultOfSecondariesAndArbiters(nwriteQuorum, numSecondariesCount,
            numJoiningSecondariesCount, numArbitersCount);
    assertTrue(!writeResult);

    serializeTest(membership);
  }

  @Test
  public void testWriteResultOfPjaLargeVolume1() throws Exception {
    Long id = 0L;
    InstanceId primary = new InstanceId(id++);
    List<InstanceId> secondaries = new ArrayList<InstanceId>();
    List<InstanceId> arbiters = new ArrayList<InstanceId>();
    List<InstanceId> inactiveSecondaries = new ArrayList<InstanceId>();
    List<InstanceId> joiningSecondaries = new ArrayList<InstanceId>();

    joiningSecondaries.add(new InstanceId(id++));
    arbiters.add(new InstanceId(id++));
    SegmentMembership membership = new SegmentMembership(new SegmentVersion(0, 0), primary,
        secondaries, arbiters,
        inactiveSecondaries, joiningSecondaries);
    final int nwriteQuorum = 3;
    int numSecondariesCount = 0;
    int numJoiningSecondariesCount = 1;
    int numArbitersCount = 1;
    boolean writeResult = membership
        .checkWriteResultOfSecondariesAndArbiters(nwriteQuorum, numSecondariesCount,
            numJoiningSecondariesCount, numArbitersCount);
    assertTrue(writeResult);

    serializeTest(membership);
  }

  @Test
  public void testBadWriteResultOfPjaLargeVolume() throws Exception {
    Long id = 0L;
    InstanceId primary = new InstanceId(id++);
    List<InstanceId> secondaries = new ArrayList<InstanceId>();
    List<InstanceId> arbiters = new ArrayList<InstanceId>();
    List<InstanceId> inactiveSecondaries = new ArrayList<InstanceId>();
    List<InstanceId> joiningSecondaries = new ArrayList<InstanceId>();

    joiningSecondaries.add(new InstanceId(id++));
    arbiters.add(new InstanceId(id++));
    SegmentMembership membership = new SegmentMembership(new SegmentVersion(0, 0), primary,
        secondaries, arbiters,
        inactiveSecondaries, joiningSecondaries);
    int numBadSecondariesCount = 0;
    int numBadJoiningSecondariesCount = 0;
    int numBadArbitersCount = 1;
    final int nwriteQuorum = 3;
    boolean writeResultBad = membership
        .checkBadWriteResultOfSecondariesAndArbiters(nwriteQuorum, numBadSecondariesCount,
            numBadJoiningSecondariesCount, numBadArbitersCount);
    assertTrue(writeResultBad);

    serializeTest(membership);
  }

  @Test
  public void testWriteResultOfpaa() throws Exception {
    Long id = 0L;
    InstanceId primary = new InstanceId(id++);
    List<InstanceId> secondaries = new ArrayList<InstanceId>();
    List<InstanceId> arbiters = new ArrayList<InstanceId>();
    List<InstanceId> inactiveSecondaries = new ArrayList<InstanceId>();
    List<InstanceId> joiningSecondaries = new ArrayList<InstanceId>();

    arbiters.add(new InstanceId(id++));
    arbiters.add(new InstanceId(id++));
    SegmentMembership membership = new SegmentMembership(new SegmentVersion(0, 0), primary,
        secondaries, arbiters,
        inactiveSecondaries, joiningSecondaries);
    final int nwriteQuorum = 3;
    int numSecondariesCount = 0;
    int numJoiningSecondariesCount = 0;
    int numArbitersCount = 2;
    boolean writeResult = membership
        .checkWriteResultOfSecondariesAndArbiters(nwriteQuorum, numSecondariesCount,
            numJoiningSecondariesCount, numArbitersCount);
    assertTrue(writeResult);

    serializeTest(membership);
  }

  @Test
  public void testBadWriteResultOfpaa() throws Exception {
    Long id = 0L;
    InstanceId primary = new InstanceId(id++);
    List<InstanceId> secondaries = new ArrayList<InstanceId>();
    List<InstanceId> arbiters = new ArrayList<InstanceId>();
    List<InstanceId> inactiveSecondaries = new ArrayList<InstanceId>();
    List<InstanceId> joiningSecondaries = new ArrayList<InstanceId>();

    arbiters.add(new InstanceId(id++));
    arbiters.add(new InstanceId(id++));
    SegmentMembership membership = new SegmentMembership(new SegmentVersion(0, 0), primary,
        secondaries, arbiters,
        inactiveSecondaries, joiningSecondaries);
    int numBadSecondariesCount = 0;
    int numBadJoiningSecondariesCount = 0;
    int numBadArbitersCount = 1;
    final int nwriteQuorum = 3;
    boolean writeResultBad = membership
        .checkBadWriteResultOfSecondariesAndArbiters(nwriteQuorum, numBadSecondariesCount,
            numBadJoiningSecondariesCount, numBadArbitersCount);
    assertTrue(writeResultBad);

    serializeTest(membership);
  }

  /**
   * split check write secondaries count and check bad write secondaries count.
   */

  @Test
  public void testBadWriteResultOfPa() throws Exception {
    Long id = 0L;
    InstanceId primary = new InstanceId(id++);
    List<InstanceId> secondaries = new ArrayList<InstanceId>();
    List<InstanceId> arbiters = new ArrayList<InstanceId>();
    arbiters.add(new InstanceId(id++));
    List<InstanceId> inactiveSecondaries = new ArrayList<InstanceId>();
    List<InstanceId> joiningSecondaries = new ArrayList<InstanceId>();
    SegmentMembership membership = new SegmentMembership(new SegmentVersion(0, 0), primary,
        secondaries, arbiters,
        inactiveSecondaries, joiningSecondaries);
    int numBadSecondariesCount = 0;
    int numBadJoiningSecondariesCount = 0;
    int numBadArbitersCount = 1;
    int nwriteQuorum = 2;
    boolean writeResultBad = membership
        .checkBadWriteResultOfSecondariesAndArbiters(nwriteQuorum, numBadSecondariesCount,
            numBadJoiningSecondariesCount, numBadArbitersCount);
    assertTrue(writeResultBad);

    serializeTest(membership);
  }

  @Test
  public void testBadWriteResultOfPsa() throws Exception {
    Long id = 0L;
    InstanceId primary = new InstanceId(id++);
    List<InstanceId> secondaries = new ArrayList<InstanceId>();
    secondaries.add(new InstanceId(id++));
    List<InstanceId> arbiters = new ArrayList<InstanceId>();
    arbiters.add(new InstanceId(id++));
    List<InstanceId> inactiveSecondaries = new ArrayList<InstanceId>();
    List<InstanceId> joiningSecondaries = new ArrayList<InstanceId>();
    SegmentMembership membership = new SegmentMembership(new SegmentVersion(0, 0), primary,
        secondaries, arbiters,
        inactiveSecondaries, joiningSecondaries);
    int numBadSecondariesCount = 1;
    int numBadJoiningSecondariesCount = 0;
    int numBadArbitersCount = 0;
    int nwriteQuorum = 2;
    boolean writeResultBad = membership
        .checkBadWriteResultOfSecondariesAndArbiters(nwriteQuorum, numBadSecondariesCount,
            numBadJoiningSecondariesCount, numBadArbitersCount);
    assertTrue(writeResultBad);

    serializeTest(membership);
  }

  @Test
  public void testBadWriteResultOfPja() throws Exception {
    Long id = 0L;
    InstanceId primary = new InstanceId(id++);
    List<InstanceId> secondaries = new ArrayList<InstanceId>();
    List<InstanceId> arbiters = new ArrayList<InstanceId>();
    arbiters.add(new InstanceId(id++));
    List<InstanceId> inactiveSecondaries = new ArrayList<InstanceId>();
    List<InstanceId> joiningSecondaries = new ArrayList<InstanceId>();
    joiningSecondaries.add(new InstanceId(id++));
    SegmentMembership membership = new SegmentMembership(new SegmentVersion(0, 0), primary,
        secondaries, arbiters,
        inactiveSecondaries, joiningSecondaries);
    int numBadJoiningSecondariesCount = 1;
    int numBadArbitersCount = 0;
    int nwriteQuorum = 2;
    int numBadSecondariesCount = 0;
    boolean writeResultBad = membership
        .checkBadWriteResultOfSecondariesAndArbiters(nwriteQuorum, numBadSecondariesCount,
            numBadJoiningSecondariesCount, numBadArbitersCount);
    assertTrue(writeResultBad);

    serializeTest(membership);
  }

  @Test
  public void testBadWriteResultOfPss() throws Exception {
    Long id = 0L;
    InstanceId primary = new InstanceId(id++);
    List<InstanceId> secondaries = new ArrayList<InstanceId>();
    secondaries.add(new InstanceId(id++));
    secondaries.add(new InstanceId(id++));
    List<InstanceId> arbiters = new ArrayList<InstanceId>();
    List<InstanceId> inactiveSecondaries = new ArrayList<InstanceId>();
    List<InstanceId> joiningSecondaries = new ArrayList<InstanceId>();
    SegmentMembership membership = new SegmentMembership(new SegmentVersion(0, 0), primary,
        secondaries, arbiters,
        inactiveSecondaries, joiningSecondaries);
    int numBadJoiningSecondariesCount = 0;
    int numBadArbitersCount = 0;
    int nwriteQuorum = 2;
    int numBadSecondariesCount = 1;
    boolean writeResultBad = membership
        .checkBadWriteResultOfSecondariesAndArbiters(nwriteQuorum, numBadSecondariesCount,
            numBadJoiningSecondariesCount, numBadArbitersCount);
    assertTrue(!writeResultBad);

    serializeTest(membership);
  }

  @Test
  public void testBadWriteResultOfPss1() throws Exception {
    Long id = 0L;
    InstanceId primary = new InstanceId(id++);
    List<InstanceId> secondaries = new ArrayList<InstanceId>();
    secondaries.add(new InstanceId(id++));
    secondaries.add(new InstanceId(id++));
    List<InstanceId> arbiters = new ArrayList<InstanceId>();
    List<InstanceId> inactiveSecondaries = new ArrayList<InstanceId>();
    List<InstanceId> joiningSecondaries = new ArrayList<InstanceId>();
    SegmentMembership membership = new SegmentMembership(new SegmentVersion(0, 0), primary,
        secondaries, arbiters,
        inactiveSecondaries, joiningSecondaries);
    int numBadSecondariesCount = 2;
    int numBadJoiningSecondariesCount = 0;
    int numBadArbitersCount = 0;
    int nwriteQuorum = 2;
    boolean writeResultBad = membership
        .checkBadWriteResultOfSecondariesAndArbiters(nwriteQuorum, numBadSecondariesCount,
            numBadJoiningSecondariesCount, numBadArbitersCount);
    assertTrue(writeResultBad);

    serializeTest(membership);
  }

  /**
   * should write to P+S.
   */
  @Test
  public void testBadWriteResultOfPsj() throws Exception {
    Long id = 0L;
    InstanceId primary = new InstanceId(id++);
    List<InstanceId> secondaries = new ArrayList<InstanceId>();
    secondaries.add(new InstanceId(id++));
    List<InstanceId> arbiters = new ArrayList<InstanceId>();
    List<InstanceId> inactiveSecondaries = new ArrayList<InstanceId>();
    List<InstanceId> joiningSecondaries = new ArrayList<InstanceId>();
    joiningSecondaries.add(new InstanceId(id++));
    SegmentMembership membership = new SegmentMembership(new SegmentVersion(0, 0), primary,
        secondaries, arbiters,
        inactiveSecondaries, joiningSecondaries);
    int nwriteQuorum = 2;
    int numBadSecondariesCount = 1;
    int numBadJoiningSecondariesCount = 0;
    int numBadArbitersCount = 0;
    boolean writeResultBad = membership
        .checkBadWriteResultOfSecondariesAndArbiters(nwriteQuorum, numBadSecondariesCount,
            numBadJoiningSecondariesCount, numBadArbitersCount);
    assertTrue(writeResultBad);

    serializeTest(membership);
  }

  /**
   * not write to P+S, but P+J.
   */
  @Test
  public void testBadWriteResultOfPsj1() {
    Long id = 0L;
    InstanceId primary = new InstanceId(id++);
    List<InstanceId> secondaries = new ArrayList<InstanceId>();
    secondaries.add(new InstanceId(id++));
    List<InstanceId> arbiters = new ArrayList<InstanceId>();
    List<InstanceId> inactiveSecondaries = new ArrayList<InstanceId>();
    List<InstanceId> joiningSecondaries = new ArrayList<InstanceId>();
    joiningSecondaries.add(new InstanceId(id++));
    SegmentMembership membership = new SegmentMembership(new SegmentVersion(0, 0), primary,
        secondaries, arbiters,
        inactiveSecondaries, joiningSecondaries);
    int numBadSecondariesCount = 0;
    int numBadJoiningSecondariesCount = 1;
    int numBadArbitersCount = 0;
    int nwriteQuorum = 2;
    boolean writeResultBad = membership
        .checkBadWriteResultOfSecondariesAndArbiters(nwriteQuorum, numBadSecondariesCount,
            numBadJoiningSecondariesCount, numBadArbitersCount);
    assertTrue(!writeResultBad);
  }

  @Test
  public void testBadWriteResultOfPs() {
    Long id = 0L;
    InstanceId primary = new InstanceId(id++);
    List<InstanceId> secondaries = new ArrayList<InstanceId>();
    secondaries.add(new InstanceId(id++));
    List<InstanceId> arbiters = new ArrayList<InstanceId>();
    List<InstanceId> inactiveSecondaries = new ArrayList<InstanceId>();
    List<InstanceId> joiningSecondaries = new ArrayList<InstanceId>();
    SegmentMembership membership = new SegmentMembership(new SegmentVersion(0, 0), primary,
        secondaries, arbiters,
        inactiveSecondaries, joiningSecondaries);
    int numBadSecondariesCount = 1;
    int numBadJoiningSecondariesCount = 0;
    int numBadArbitersCount = 0;
    int nwriteQuorum = 2;
    boolean writeResultBad = membership
        .checkBadWriteResultOfSecondariesAndArbiters(nwriteQuorum, numBadSecondariesCount,
            numBadJoiningSecondariesCount, numBadArbitersCount);
    assertTrue(writeResultBad);
  }

  @Test
  public void testBadWriteResultOfPj() throws Exception {
    Long id = 0L;
    InstanceId primary = new InstanceId(id++);
    List<InstanceId> secondaries = new ArrayList<InstanceId>();
    List<InstanceId> arbiters = new ArrayList<InstanceId>();
    List<InstanceId> inactiveSecondaries = new ArrayList<InstanceId>();
    List<InstanceId> joiningSecondaries = new ArrayList<InstanceId>();
    joiningSecondaries.add(new InstanceId(id++));
    SegmentMembership membership = new SegmentMembership(new SegmentVersion(0, 0), primary,
        secondaries, arbiters,
        inactiveSecondaries, joiningSecondaries);
    int numBadJoiningSecondariesCount = 1;
    int numBadArbitersCount = 0;
    int nwriteQuorum = 2;
    int numBadSecondariesCount = 0;
    boolean writeResultBad = membership
        .checkBadWriteResultOfSecondariesAndArbiters(nwriteQuorum, numBadSecondariesCount,
            numBadJoiningSecondariesCount, numBadArbitersCount);
    assertTrue(writeResultBad);

    serializeTest(membership);
  }

  @Test
  public void testGetSegmentForm() {
    //test error

    Long id = 0L;

    // PSS(1),
    InstanceId primary = new InstanceId(id++);
    List<InstanceId> secondaries = new ArrayList<InstanceId>();
    List<InstanceId> arbiters = new ArrayList<InstanceId>();
    List<InstanceId> inactiveSecondaries = new ArrayList<InstanceId>();
    List<InstanceId> joiningSecondaries = new ArrayList<InstanceId>();
    secondaries.add(new InstanceId(id++));
    secondaries.add(new InstanceId(id++));
    SegmentMembership membership = new SegmentMembership(new SegmentVersion(0, 0), primary,
        secondaries, arbiters,
        inactiveSecondaries, joiningSecondaries);

    SegmentForm segmentForm = SegmentForm.getSegmentForm(membership, VolumeType.REGULAR);
    assertTrue(segmentForm == SegmentForm.PSS);

    boolean caughtException = false;
    try {
      segmentForm = SegmentForm.getSegmentForm(membership, VolumeType.SMALL);
    } catch (Exception e) {
      caughtException = true;
    }
    assertTrue(caughtException);

    // PSJ(2),
    caughtException = false;
    secondaries.clear();
    arbiters.clear();
    inactiveSecondaries.clear();
    joiningSecondaries.clear();

    secondaries.add(new InstanceId(id++));
    joiningSecondaries.add(new InstanceId(id++));

    membership = new SegmentMembership(new SegmentVersion(0, 0), primary, secondaries, arbiters,
        inactiveSecondaries, joiningSecondaries);

    segmentForm = SegmentForm.getSegmentForm(membership, VolumeType.REGULAR);
    assertTrue(segmentForm == SegmentForm.PSJ);

    try {
      segmentForm = SegmentForm.getSegmentForm(membership, VolumeType.SMALL);
    } catch (Exception e) {
      caughtException = true;
    }
    assertTrue(caughtException);

    // PSI(3),
    caughtException = false;
    secondaries.clear();
    arbiters.clear();
    inactiveSecondaries.clear();
    joiningSecondaries.clear();

    secondaries.add(new InstanceId(id++));
    inactiveSecondaries.add(new InstanceId(id++));

    membership = new SegmentMembership(new SegmentVersion(0, 0), primary, secondaries, arbiters,
        inactiveSecondaries, joiningSecondaries);

    segmentForm = SegmentForm.getSegmentForm(membership, VolumeType.REGULAR);
    assertTrue(segmentForm == SegmentForm.PSI);

    try {
      segmentForm = SegmentForm.getSegmentForm(membership, VolumeType.SMALL);
    } catch (Exception e) {
      caughtException = true;
    }
    assertTrue(!caughtException);
    assertTrue(segmentForm == SegmentForm.TPS);
    // PJI(4),
    caughtException = false;
    secondaries.clear();
    arbiters.clear();
    inactiveSecondaries.clear();
    joiningSecondaries.clear();

    joiningSecondaries.add(new InstanceId(id++));
    inactiveSecondaries.add(new InstanceId(id++));

    membership = new SegmentMembership(new SegmentVersion(0, 0), primary, secondaries, arbiters,
        inactiveSecondaries, joiningSecondaries);

    segmentForm = SegmentForm.getSegmentForm(membership, VolumeType.REGULAR);
    assertTrue(segmentForm == SegmentForm.PJI);

    try {
      segmentForm = SegmentForm.getSegmentForm(membership, VolumeType.SMALL);
    } catch (Exception e) {
      caughtException = true;
    }
    assertTrue(!caughtException);
    assertTrue(segmentForm == SegmentForm.TPJ);
    // PJJ(5),
    caughtException = false;
    secondaries.clear();
    arbiters.clear();
    inactiveSecondaries.clear();
    joiningSecondaries.clear();

    joiningSecondaries.add(new InstanceId(id++));
    joiningSecondaries.add(new InstanceId(id++));

    membership = new SegmentMembership(new SegmentVersion(0, 0), primary, secondaries, arbiters,
        inactiveSecondaries, joiningSecondaries);

    segmentForm = SegmentForm.getSegmentForm(membership, VolumeType.REGULAR);
    assertTrue(segmentForm == SegmentForm.PJJ);

    try {
      segmentForm = SegmentForm.getSegmentForm(membership, VolumeType.SMALL);
    } catch (Exception e) {
      caughtException = true;
    }
    assertTrue(caughtException);
    // PII(6),
    caughtException = false;
    secondaries.clear();
    arbiters.clear();
    inactiveSecondaries.clear();
    joiningSecondaries.clear();

    inactiveSecondaries.add(new InstanceId(id++));
    inactiveSecondaries.add(new InstanceId(id++));

    membership = new SegmentMembership(new SegmentVersion(0, 0), primary, secondaries, arbiters,
        inactiveSecondaries, joiningSecondaries);

    segmentForm = SegmentForm.getSegmentForm(membership, VolumeType.REGULAR);
    assertTrue(segmentForm == SegmentForm.PII);

    try {
      segmentForm = SegmentForm.getSegmentForm(membership, VolumeType.SMALL);
    } catch (Exception e) {
      caughtException = true;
    }
    assertTrue(!caughtException);
    assertTrue(segmentForm == SegmentForm.TPI);
    // PS(7),
    secondaries.clear();
    arbiters.clear();
    inactiveSecondaries.clear();
    joiningSecondaries.clear();

    secondaries.add(new InstanceId(id++));

    membership = new SegmentMembership(new SegmentVersion(0, 0), primary, secondaries, arbiters,
        inactiveSecondaries, joiningSecondaries);

    segmentForm = SegmentForm.getSegmentForm(membership, VolumeType.REGULAR);
    assertTrue(segmentForm == SegmentForm.PS);

    segmentForm = SegmentForm.getSegmentForm(membership, VolumeType.SMALL);
    assertTrue(segmentForm == SegmentForm.TPS);
    // PJ(8),
    secondaries.clear();
    arbiters.clear();
    inactiveSecondaries.clear();
    joiningSecondaries.clear();

    joiningSecondaries.add(new InstanceId(id++));

    membership = new SegmentMembership(new SegmentVersion(0, 0), primary, secondaries, arbiters,
        inactiveSecondaries, joiningSecondaries);

    segmentForm = SegmentForm.getSegmentForm(membership, VolumeType.REGULAR);
    assertTrue(segmentForm == SegmentForm.PJ);

    segmentForm = SegmentForm.getSegmentForm(membership, VolumeType.SMALL);
    assertTrue(segmentForm == SegmentForm.TPJ);
    // PI(9),
    secondaries.clear();
    arbiters.clear();
    inactiveSecondaries.clear();
    joiningSecondaries.clear();

    inactiveSecondaries.add(new InstanceId(id++));

    membership = new SegmentMembership(new SegmentVersion(0, 0), primary, secondaries, arbiters,
        inactiveSecondaries, joiningSecondaries);

    segmentForm = SegmentForm.getSegmentForm(membership, VolumeType.REGULAR);
    assertTrue(segmentForm == SegmentForm.PI);

    segmentForm = SegmentForm.getSegmentForm(membership, VolumeType.SMALL);
    assertTrue(segmentForm == SegmentForm.TPI);

    // Psa(10),
    caughtException = false;
    secondaries.clear();
    arbiters.clear();
    inactiveSecondaries.clear();
    joiningSecondaries.clear();

    secondaries.add(new InstanceId(id++));
    arbiters.add(new InstanceId(id++));

    membership = new SegmentMembership(new SegmentVersion(0, 0), primary, secondaries, arbiters,
        inactiveSecondaries, joiningSecondaries);

    segmentForm = SegmentForm.getSegmentForm(membership, VolumeType.SMALL);
    assertTrue(segmentForm == SegmentForm.PSA);

    try {
      segmentForm = SegmentForm.getSegmentForm(membership, VolumeType.REGULAR);
    } catch (Exception e) {
      caughtException = true;
    }
    assertTrue(caughtException);

    // PJA(11),
    caughtException = false;
    secondaries.clear();
    arbiters.clear();
    inactiveSecondaries.clear();
    joiningSecondaries.clear();

    joiningSecondaries.add(new InstanceId(id++));
    arbiters.add(new InstanceId(id++));

    membership = new SegmentMembership(new SegmentVersion(0, 0), primary, secondaries, arbiters,
        inactiveSecondaries, joiningSecondaries);

    segmentForm = SegmentForm.getSegmentForm(membership, VolumeType.SMALL);
    assertTrue(segmentForm == SegmentForm.PJA);

    try {
      segmentForm = SegmentForm.getSegmentForm(membership, VolumeType.REGULAR);
    } catch (Exception e) {
      caughtException = true;
    }
    assertTrue(caughtException);
    // PIA(12),
    caughtException = false;
    secondaries.clear();
    arbiters.clear();
    inactiveSecondaries.clear();
    joiningSecondaries.clear();

    inactiveSecondaries.add(new InstanceId(id++));
    arbiters.add(new InstanceId(id++));

    membership = new SegmentMembership(new SegmentVersion(0, 0), primary, secondaries, arbiters,
        inactiveSecondaries, joiningSecondaries);

    segmentForm = SegmentForm.getSegmentForm(membership, VolumeType.SMALL);
    assertTrue(segmentForm == SegmentForm.PIA);

    try {
      segmentForm = SegmentForm.getSegmentForm(membership, VolumeType.REGULAR);
    } catch (Exception e) {
      caughtException = true;
    }
    assertTrue(caughtException);

    // PA(13),
    caughtException = false;
    secondaries.clear();
    arbiters.clear();
    inactiveSecondaries.clear();
    joiningSecondaries.clear();

    arbiters.add(new InstanceId(id++));

    membership = new SegmentMembership(new SegmentVersion(0, 0), primary, secondaries, arbiters,
        inactiveSecondaries, joiningSecondaries);

    segmentForm = SegmentForm.getSegmentForm(membership, VolumeType.SMALL);
    assertTrue(segmentForm == SegmentForm.PA);

    try {
      segmentForm = SegmentForm.getSegmentForm(membership, VolumeType.REGULAR);
    } catch (Exception e) {
      caughtException = true;
    }
    assertTrue(caughtException);
    // TPS(14),
    secondaries.clear();
    arbiters.clear();
    inactiveSecondaries.clear();
    joiningSecondaries.clear();

    secondaries.add(new InstanceId(id++));

    membership = new SegmentMembership(new SegmentVersion(0, 0), primary, secondaries, arbiters,
        inactiveSecondaries, joiningSecondaries);

    segmentForm = SegmentForm.getSegmentForm(membership, VolumeType.SMALL);
    assertTrue(segmentForm == SegmentForm.TPS);

    segmentForm = SegmentForm.getSegmentForm(membership, VolumeType.REGULAR);
    assertTrue(segmentForm == SegmentForm.PS);
    // TPJ(15),
    secondaries.clear();
    arbiters.clear();
    inactiveSecondaries.clear();
    joiningSecondaries.clear();

    joiningSecondaries.add(new InstanceId(id++));

    membership = new SegmentMembership(new SegmentVersion(0, 0), primary, secondaries, arbiters,
        inactiveSecondaries, joiningSecondaries);

    segmentForm = SegmentForm.getSegmentForm(membership, VolumeType.SMALL);
    assertTrue(segmentForm == SegmentForm.TPJ);

    segmentForm = SegmentForm.getSegmentForm(membership, VolumeType.REGULAR);
    assertTrue(segmentForm == SegmentForm.PJ);
    // TPI(16);
    secondaries.clear();
    arbiters.clear();
    inactiveSecondaries.clear();
    joiningSecondaries.clear();

    inactiveSecondaries.add(new InstanceId(id++));

    membership = new SegmentMembership(new SegmentVersion(0, 0), primary, secondaries, arbiters,
        inactiveSecondaries, joiningSecondaries);

    segmentForm = SegmentForm.getSegmentForm(membership, VolumeType.SMALL);
    assertTrue(segmentForm == SegmentForm.TPI);

    segmentForm = SegmentForm.getSegmentForm(membership, VolumeType.REGULAR);
    assertTrue(segmentForm == SegmentForm.PI);
  }

  public void makeClean() {
    primaryNew = null;
    secondariesNew.clear();
    joiningSecondariesNew.clear();
    inactiveSecondariesNew.clear();
    arbitersNew.clear();
  }

  public SegmentForm makeSegmentForm(SegmentForm segmentForm) {
    makeClean();
    String name = segmentForm.name();
    for (int i = 0; i < name.length(); i++) {
      switch (name.charAt(i)) {
        case 'P':
          primaryNew = new InstanceId(instanceId++);
          break;
        case 'S':
          InstanceId secondary = new InstanceId(instanceId++);
          secondariesNew.add(secondary);
          break;
        case 'J':
          InstanceId joiningSecondary = new InstanceId(instanceId++);
          joiningSecondariesNew.add(joiningSecondary);
          break;
        case 'A':
          InstanceId arbiter = new InstanceId(instanceId++);
          arbitersNew.add(arbiter);
          break;
        case 'I':
          InstanceId inactive = new InstanceId(instanceId++);
          inactiveSecondariesNew.add(inactive);
          break;
        default:
          break;
      }
    }

    SegmentMembership membership = new SegmentMembership(new SegmentVersion(0, 0), primaryNew,
        secondariesNew, arbitersNew,
        inactiveSecondariesNew, joiningSecondariesNew);
    SegmentForm segmentFormGet = SegmentForm.getSegmentForm(membership, VolumeType.LARGE);
    return segmentFormGet;
  }

  public void makeSegmentFormError(String name, VolumeType volumeType, boolean exception) {
    makeClean();
    SegmentForm segmentForm;
    boolean caughtException = exception;

    for (int i = 0; i < name.length(); i++) {
      switch (name.charAt(i)) {
        case 'P':
          primaryNew = new InstanceId(instanceId++);
          break;
        case 'S':
          InstanceId secondary = new InstanceId(instanceId++);
          secondariesNew.add(secondary);
          break;
        case 'J':
          InstanceId joiningSecondary = new InstanceId(instanceId++);
          joiningSecondariesNew.add(joiningSecondary);
          break;
        case 'A':
          InstanceId arbiter = new InstanceId(instanceId++);
          arbitersNew.add(arbiter);
          break;
        case 'I':
          InstanceId inactive = new InstanceId(instanceId++);
          inactiveSecondariesNew.add(inactive);
          break;
        default:
          break;
      }
    }

    SegmentMembership membership = new SegmentMembership(new SegmentVersion(0, 0), primaryNew,
        secondariesNew, arbitersNew,
        inactiveSecondariesNew, joiningSecondariesNew);

    try {
      segmentForm = SegmentForm.getSegmentForm(membership, volumeType);
    } catch (Exception e) {
      caughtException = exception;
    }
    if (exception) {
      //Find exception
      assertTrue(caughtException);
    } else {
      assertTrue(!caughtException);
    }

  }

  @Test
  public void testGetSegmentFormPssaa() {
    //set true,means the is a exception
    makeSegmentFormError("PSSS", VolumeType.REGULAR, true);
    makeSegmentFormError("PSII", VolumeType.REGULAR, true);
    makeSegmentFormError("PSA", VolumeType.REGULAR, true);
    makeSegmentFormError("PSIIII", VolumeType.REGULAR, false);
    makeSegmentFormError("PSIIII", VolumeType.SMALL, false);
    makeSegmentFormError("PSAIIII", VolumeType.SMALL, false);
    makeSegmentFormError("PJAI", VolumeType.SMALL, false);

    SegmentForm segmentForm;
    //Pssaa
    segmentForm = makeSegmentForm(SegmentForm.PSSAA);
    assertTrue(segmentForm == SegmentForm.PSSAA);

    //PSSAI
    segmentForm = null;
    segmentForm = makeSegmentForm(SegmentForm.PSSAI);
    assertTrue(segmentForm == SegmentForm.PSSAI);

    //PSSII
    segmentForm = null;
    segmentForm = makeSegmentForm(SegmentForm.PSSII);
    assertTrue(segmentForm == SegmentForm.PSSII);

    //PSJAA
    segmentForm = null;
    segmentForm = makeSegmentForm(SegmentForm.PSJAA);
    assertTrue(segmentForm == SegmentForm.PSJAA);

    //PSJAI
    segmentForm = null;
    segmentForm = makeSegmentForm(SegmentForm.PSJAI);
    assertTrue(segmentForm == SegmentForm.PSJAI);

    //PSJII
    segmentForm = null;
    segmentForm = makeSegmentForm(SegmentForm.PSJII);
    assertTrue(segmentForm == SegmentForm.PSJII);

    //PSIAA
    segmentForm = null;
    segmentForm = makeSegmentForm(SegmentForm.PSIAA);
    assertTrue(segmentForm == SegmentForm.PSIAA);

    //PSIAI
    segmentForm = null;
    segmentForm = makeSegmentForm(SegmentForm.PSIAI);
    assertTrue(segmentForm == SegmentForm.PSIAI);

    //PSIII
    segmentForm = null;
    segmentForm = makeSegmentForm(SegmentForm.PSIII);
    assertTrue(segmentForm == SegmentForm.PSIII);

    //PJJAA
    segmentForm = null;
    segmentForm = makeSegmentForm(SegmentForm.PJJAA);
    assertTrue(segmentForm == SegmentForm.PJJAA);

    //PJJAI
    segmentForm = null;
    segmentForm = makeSegmentForm(SegmentForm.PJJAI);
    assertTrue(segmentForm == SegmentForm.PJJAI);

    //PJJII
    segmentForm = null;
    segmentForm = makeSegmentForm(SegmentForm.PJJII);
    assertTrue(segmentForm == SegmentForm.PJJII);

    //PIIAA
    segmentForm = null;
    segmentForm = makeSegmentForm(SegmentForm.PIIAA);
    assertTrue(segmentForm == SegmentForm.PIIAA);

    //PIIAI
    segmentForm = null;
    segmentForm = makeSegmentForm(SegmentForm.PIIAI);
    assertTrue(segmentForm == SegmentForm.PIIAI);

    //PIIII
    segmentForm = null;
    segmentForm = makeSegmentForm(SegmentForm.PIIII);
    assertTrue(segmentForm == SegmentForm.PIIII);

    //PSAA
    segmentForm = null;
    segmentForm = makeSegmentForm(SegmentForm.PSAA);
    assertTrue(segmentForm == SegmentForm.PSAA);

    //PSAI
    segmentForm = null;
    segmentForm = makeSegmentForm(SegmentForm.PSAI);
    assertTrue(segmentForm == SegmentForm.PSAI);

    //PSII
    segmentForm = null;
    segmentForm = makeSegmentForm(SegmentForm.PSII);
    assertTrue(segmentForm == SegmentForm.PSII);

    //PIAA
    segmentForm = null;
    segmentForm = makeSegmentForm(SegmentForm.PIAA);
    assertTrue(segmentForm == SegmentForm.PIAA);

    //PIAI
    segmentForm = null;
    segmentForm = makeSegmentForm(SegmentForm.PIAI);
    assertTrue(segmentForm == SegmentForm.PIAI);

    //PIII
    segmentForm = null;
    segmentForm = makeSegmentForm(SegmentForm.PIII);
    assertTrue(segmentForm == SegmentForm.PIII);

    //PJAA
    segmentForm = null;
    segmentForm = makeSegmentForm(SegmentForm.PJAA);
    assertTrue(segmentForm == SegmentForm.PJAA);

    //PJII
    segmentForm = null;
    segmentForm = makeSegmentForm(SegmentForm.PJII);
    assertTrue(segmentForm == SegmentForm.PJII);

    //PAA
    segmentForm = null;
    segmentForm = makeSegmentForm(SegmentForm.PAA);
    assertTrue(segmentForm == SegmentForm.PAA);
  }

  @Test
  public void testGetSegmentFormWithManyInactiveSecondaries() throws Exception {
    Long id = 0L;

    List<InstanceId> inactiveSecondaries = new ArrayList<InstanceId>();
    inactiveSecondaries.add(new InstanceId(id++));
    inactiveSecondaries.add(new InstanceId(id++));
    inactiveSecondaries.add(new InstanceId(id++));
    inactiveSecondaries.add(new InstanceId(id++));

    // PSS(1),
    InstanceId primary = new InstanceId(id++);
    List<InstanceId> secondaries = new ArrayList<InstanceId>();
    List<InstanceId> arbiters = new ArrayList<InstanceId>();

    List<InstanceId> joiningSecondaries = new ArrayList<InstanceId>();
    secondaries.add(new InstanceId(id++));
    secondaries.add(new InstanceId(id++));
    SegmentMembership membership = new SegmentMembership(new SegmentVersion(0, 0), primary,
        secondaries, arbiters,
        inactiveSecondaries, joiningSecondaries);

    SegmentForm segmentForm = SegmentForm.getSegmentForm(membership, VolumeType.REGULAR);
    assertTrue(segmentForm == SegmentForm.PSS);

    boolean caughtException = false;
    try {
      segmentForm = SegmentForm.getSegmentForm(membership, VolumeType.SMALL);
    } catch (Exception e) {
      caughtException = true;
    }
    assertTrue(caughtException);

    // PSJ(2),
    caughtException = false;
    secondaries.clear();
    arbiters.clear();
    joiningSecondaries.clear();

    secondaries.add(new InstanceId(id++));
    joiningSecondaries.add(new InstanceId(id++));

    membership = new SegmentMembership(new SegmentVersion(0, 0), primary, secondaries, arbiters,
        inactiveSecondaries, joiningSecondaries);

    segmentForm = SegmentForm.getSegmentForm(membership, VolumeType.REGULAR);
    assertTrue(segmentForm == SegmentForm.PSJ);

    try {
      segmentForm = SegmentForm.getSegmentForm(membership, VolumeType.SMALL);
    } catch (Exception e) {
      caughtException = true;
    }
    assertTrue(caughtException);

    // PSI(3),
    caughtException = false;
    secondaries.clear();
    arbiters.clear();
    joiningSecondaries.clear();

    secondaries.add(new InstanceId(id++));
    inactiveSecondaries.add(new InstanceId(id++));

    membership = new SegmentMembership(new SegmentVersion(0, 0), primary, secondaries, arbiters,
        inactiveSecondaries, joiningSecondaries);

    segmentForm = SegmentForm.getSegmentForm(membership, VolumeType.REGULAR);
    assertTrue(segmentForm == SegmentForm.PSI);

    try {
      segmentForm = SegmentForm.getSegmentForm(membership, VolumeType.SMALL);
    } catch (Exception e) {
      caughtException = true;
    }
    assertTrue(!caughtException);
    assertTrue(segmentForm == SegmentForm.TPS);
    // PJI(4),
    caughtException = false;
    secondaries.clear();
    arbiters.clear();
    joiningSecondaries.clear();

    joiningSecondaries.add(new InstanceId(id++));
    inactiveSecondaries.add(new InstanceId(id++));

    membership = new SegmentMembership(new SegmentVersion(0, 0), primary, secondaries, arbiters,
        inactiveSecondaries, joiningSecondaries);

    segmentForm = SegmentForm.getSegmentForm(membership, VolumeType.REGULAR);
    assertTrue(segmentForm == SegmentForm.PJI);

    try {
      segmentForm = SegmentForm.getSegmentForm(membership, VolumeType.SMALL);
    } catch (Exception e) {
      caughtException = true;
    }
    assertTrue(!caughtException);
    assertTrue(segmentForm == SegmentForm.TPJ);
    // PJJ(5),
    caughtException = false;
    secondaries.clear();
    arbiters.clear();
    joiningSecondaries.clear();

    joiningSecondaries.add(new InstanceId(id++));
    joiningSecondaries.add(new InstanceId(id++));

    membership = new SegmentMembership(new SegmentVersion(0, 0), primary, secondaries, arbiters,
        inactiveSecondaries, joiningSecondaries);

    segmentForm = SegmentForm.getSegmentForm(membership, VolumeType.REGULAR);
    assertTrue(segmentForm == SegmentForm.PJJ);

    try {
      segmentForm = SegmentForm.getSegmentForm(membership, VolumeType.SMALL);
    } catch (Exception e) {
      caughtException = true;
    }
    assertTrue(caughtException);
    // PII(6),
    caughtException = false;
    secondaries.clear();
    arbiters.clear();
    joiningSecondaries.clear();

    inactiveSecondaries.add(new InstanceId(id++));
    inactiveSecondaries.add(new InstanceId(id++));

    membership = new SegmentMembership(new SegmentVersion(0, 0), primary, secondaries, arbiters,
        inactiveSecondaries, joiningSecondaries);

    segmentForm = SegmentForm.getSegmentForm(membership, VolumeType.REGULAR);
    assertTrue(segmentForm == SegmentForm.PII);

    try {
      segmentForm = SegmentForm.getSegmentForm(membership, VolumeType.SMALL);
    } catch (Exception e) {
      caughtException = true;
    }
    assertTrue(!caughtException);
    assertTrue(segmentForm == SegmentForm.TPI);
    // PS(7),
    secondaries.clear();
    arbiters.clear();
    joiningSecondaries.clear();

    secondaries.add(new InstanceId(id++));

    membership = new SegmentMembership(new SegmentVersion(0, 0), primary, secondaries, arbiters,
        inactiveSecondaries, joiningSecondaries);

    segmentForm = SegmentForm.getSegmentForm(membership, VolumeType.REGULAR);
    assertTrue(segmentForm == SegmentForm.PSI);

    segmentForm = SegmentForm.getSegmentForm(membership, VolumeType.SMALL);
    assertTrue(segmentForm == SegmentForm.TPS);
    // PJ(8),
    secondaries.clear();
    arbiters.clear();
    joiningSecondaries.clear();

    joiningSecondaries.add(new InstanceId(id++));

    membership = new SegmentMembership(new SegmentVersion(0, 0), primary, secondaries, arbiters,
        inactiveSecondaries, joiningSecondaries);

    segmentForm = SegmentForm.getSegmentForm(membership, VolumeType.REGULAR);
    assertTrue(segmentForm == SegmentForm.PJI);

    segmentForm = SegmentForm.getSegmentForm(membership, VolumeType.SMALL);
    assertTrue(segmentForm == SegmentForm.TPJ);
    // PI(9),
    secondaries.clear();
    arbiters.clear();
    joiningSecondaries.clear();

    inactiveSecondaries.add(new InstanceId(id++));

    membership = new SegmentMembership(new SegmentVersion(0, 0), primary, secondaries, arbiters,
        inactiveSecondaries, joiningSecondaries);

    segmentForm = SegmentForm.getSegmentForm(membership, VolumeType.REGULAR);
    assertTrue(segmentForm == SegmentForm.PII);

    segmentForm = SegmentForm.getSegmentForm(membership, VolumeType.SMALL);
    assertTrue(segmentForm == SegmentForm.TPI);

    // PSA(10),
    caughtException = false;
    secondaries.clear();
    arbiters.clear();
    joiningSecondaries.clear();

    secondaries.add(new InstanceId(id++));
    arbiters.add(new InstanceId(id++));

    membership = new SegmentMembership(new SegmentVersion(0, 0), primary, secondaries, arbiters,
        inactiveSecondaries, joiningSecondaries);

    segmentForm = SegmentForm.getSegmentForm(membership, VolumeType.SMALL);
    assertTrue(segmentForm == SegmentForm.PSA);

    try {
      segmentForm = SegmentForm.getSegmentForm(membership, VolumeType.REGULAR);
    } catch (Exception e) {
      caughtException = true;
    }
    assertTrue(caughtException);

    // PJA(11),
    caughtException = false;
    secondaries.clear();
    arbiters.clear();
    joiningSecondaries.clear();

    joiningSecondaries.add(new InstanceId(id++));
    arbiters.add(new InstanceId(id++));

    membership = new SegmentMembership(new SegmentVersion(0, 0), primary, secondaries, arbiters,
        inactiveSecondaries, joiningSecondaries);

    segmentForm = SegmentForm.getSegmentForm(membership, VolumeType.SMALL);
    assertTrue(segmentForm == SegmentForm.PJA);

    try {
      segmentForm = SegmentForm.getSegmentForm(membership, VolumeType.REGULAR);
    } catch (Exception e) {
      caughtException = true;
    }
    assertTrue(caughtException);
    // PIA(12),
    caughtException = false;
    secondaries.clear();
    arbiters.clear();
    joiningSecondaries.clear();

    inactiveSecondaries.add(new InstanceId(id++));
    arbiters.add(new InstanceId(id++));

    membership = new SegmentMembership(new SegmentVersion(0, 0), primary, secondaries, arbiters,
        inactiveSecondaries, joiningSecondaries);

    segmentForm = SegmentForm.getSegmentForm(membership, VolumeType.SMALL);
    assertTrue(segmentForm == SegmentForm.PIA);

    try {
      segmentForm = SegmentForm.getSegmentForm(membership, VolumeType.REGULAR);
    } catch (Exception e) {
      caughtException = true;
    }
    assertTrue(caughtException);

    // PA(13),
    caughtException = false;
    secondaries.clear();
    arbiters.clear();
    joiningSecondaries.clear();

    arbiters.add(new InstanceId(id++));

    membership = new SegmentMembership(new SegmentVersion(0, 0), primary, secondaries, arbiters,
        inactiveSecondaries, joiningSecondaries);

    segmentForm = SegmentForm.getSegmentForm(membership, VolumeType.SMALL);
    assertTrue(segmentForm == SegmentForm.PIA);

    try {
      segmentForm = SegmentForm.getSegmentForm(membership, VolumeType.REGULAR);
    } catch (Exception e) {
      caughtException = true;
    }
    assertTrue(caughtException);
    // TPS(14),
    secondaries.clear();
    arbiters.clear();
    joiningSecondaries.clear();

    secondaries.add(new InstanceId(id++));

    membership = new SegmentMembership(new SegmentVersion(0, 0), primary, secondaries, arbiters,
        inactiveSecondaries, joiningSecondaries);

    segmentForm = SegmentForm.getSegmentForm(membership, VolumeType.SMALL);
    assertTrue(segmentForm == SegmentForm.TPS);

    segmentForm = SegmentForm.getSegmentForm(membership, VolumeType.REGULAR);
    assertTrue(segmentForm == SegmentForm.PSI);
    // TPJ(15),
    secondaries.clear();
    arbiters.clear();
    joiningSecondaries.clear();

    joiningSecondaries.add(new InstanceId(id++));

    membership = new SegmentMembership(new SegmentVersion(0, 0), primary, secondaries, arbiters,
        inactiveSecondaries, joiningSecondaries);

    segmentForm = SegmentForm.getSegmentForm(membership, VolumeType.SMALL);
    assertTrue(segmentForm == SegmentForm.TPJ);

    segmentForm = SegmentForm.getSegmentForm(membership, VolumeType.REGULAR);
    assertTrue(segmentForm == SegmentForm.PJI);
    // TPI(16);
    secondaries.clear();
    arbiters.clear();
    joiningSecondaries.clear();

    inactiveSecondaries.add(new InstanceId(id++));

    membership = new SegmentMembership(new SegmentVersion(0, 0), primary, secondaries, arbiters,
        inactiveSecondaries, joiningSecondaries);

    segmentForm = SegmentForm.getSegmentForm(membership, VolumeType.SMALL);
    assertTrue(segmentForm == SegmentForm.TPI);

    segmentForm = SegmentForm.getSegmentForm(membership, VolumeType.REGULAR);
    assertTrue(segmentForm == SegmentForm.PII);

    serializeTest(membership);
  }

  @Test
  public void testMergeMemberStatus() throws Exception {
    final Long requestId = RequestIdBuilder.get();
    Long id = 0L;
    final int epoch = 0;
    final int generation = 0;

    final InstanceId primaryId = new InstanceId(id);
    List<InstanceId> secondaries1 = new ArrayList<>();
    final List<InstanceId> arbiters1 = new ArrayList<>();
    final List<InstanceId> joiningSecondaries1 = new ArrayList<>();

    final List<InstanceId> secondaries2 = new ArrayList<>();
    final List<InstanceId> arbiters2 = new ArrayList<>();
    final List<InstanceId> joiningSecondaries2 = new ArrayList<>();

    id++;
    InstanceId secondaryId = new InstanceId(id);
    secondaries1.add(secondaryId);
    secondaries2.add(secondaryId);

    id++;
    InstanceId arbiterId = new InstanceId(id);
    arbiters1.add(arbiterId);
    arbiters2.add(arbiterId);

    SegmentMembership membership1 = new SegmentMembership(new SegmentVersion(epoch, generation),
        primaryId,
        secondaries1, arbiters1, null, joiningSecondaries1);
    SegmentMembership membership2 = new SegmentMembership(new SegmentVersion(epoch, generation),
        primaryId,
        secondaries2, arbiters2, null, joiningSecondaries2);

    assertTrue(membership1.compareTo(membership2) == 0);

    // mark membership1 primary down
    MemberIoStatus primary1Status = membership1.getMemberIoStatus(primaryId);
    assertTrue(primary1Status == MemberIoStatus.Primary);

    membership1.markMemberIoStatus(primaryId, MemberIoStatus.PrimaryDown);

    membership2.mergeMemberStatus(requestId, membership1);

    MemberIoStatus primary2Status = membership2.getMemberIoStatus(primaryId);
    assertTrue(primary2Status == MemberIoStatus.PrimaryDown);
    MemberIoStatus secondary2Status = membership2.getMemberIoStatus(secondaryId);
    assertTrue(secondary2Status == MemberIoStatus.Secondary);

    primary1Status = membership1.getMemberIoStatus(primaryId);
    assertTrue(primary1Status == MemberIoStatus.PrimaryDown);
    MemberIoStatus secondary1Status = membership1.getMemberIoStatus(secondaryId);
    assertTrue(secondary1Status == MemberIoStatus.Secondary);

    // mark membership1 secondary down
    secondary1Status = membership1.getMemberIoStatus(secondaryId);
    assertTrue(secondary1Status == MemberIoStatus.Secondary);
    membership1.markMemberIoStatus(secondaryId, MemberIoStatus.SecondaryDown);

    membership2.mergeMemberStatus(requestId, membership1);

    primary2Status = membership2.getMemberIoStatus(primaryId);
    assertTrue(primary2Status == MemberIoStatus.PrimaryDown);
    secondary2Status = membership2.getMemberIoStatus(secondaryId);
    assertTrue(secondary2Status == MemberIoStatus.SecondaryDown);

    primary1Status = membership1.getMemberIoStatus(primaryId);
    assertTrue(primary1Status == MemberIoStatus.PrimaryDown);
    secondary1Status = membership1.getMemberIoStatus(secondaryId);
    assertTrue(secondary1Status == MemberIoStatus.SecondaryDown);

    // do not mark membership1 arbiter
    MemberIoStatus arbiter1Status = membership1.getMemberIoStatus(arbiterId);
    assertTrue(arbiter1Status == MemberIoStatus.Arbiter);

    membership2.mergeMemberStatus(requestId, membership1);

    primary2Status = membership2.getMemberIoStatus(primaryId);
    assertTrue(primary2Status == MemberIoStatus.PrimaryDown);
    secondary2Status = membership2.getMemberIoStatus(secondaryId);
    assertTrue(secondary2Status == MemberIoStatus.SecondaryDown);
    MemberIoStatus arbiter2Status = membership2.getMemberIoStatus(arbiterId);
    assertTrue(arbiter2Status == MemberIoStatus.Arbiter);

    serializeTest(membership1);
    serializeTest(membership2);
  }

  @Test
  public void addSecondaryCandidate() throws Exception {
    int index = 0;
    SegmentVersion version = new SegmentVersion(0, 0);
    InstanceId primary = new InstanceId(index++);
    InstanceId tempPrimary = new InstanceId(index++);
    InstanceId secondaryCandidate = new InstanceId(index++);
    InstanceId secondary1 = new InstanceId(index++);
    InstanceId secondary2 = new InstanceId(index++);
    InstanceId arbiter1 = new InstanceId(index++);
    InstanceId arbiter2 = new InstanceId(index++);
    InstanceId inactiveSecondary1 = new InstanceId(index++);
    InstanceId inactiveSecondary2 = new InstanceId(index++);
    InstanceId joiningSecondary1 = new InstanceId(index++);
    InstanceId joiningSecondary2 = new InstanceId(index++);

    SegmentMembership membership = new SegmentMembership(version, primary, tempPrimary,
        Sets.newHashSet(secondary1, secondary2), Sets.newHashSet(arbiter1, arbiter2),
        Sets.newHashSet(inactiveSecondary1, inactiveSecondary2),
        Sets.newHashSet(joiningSecondary1, joiningSecondary2), null, null);

    assertNull(membership.getSecondaryCandidate());

    SegmentMembership newMembership = membership.addSecondaryCandidate(secondaryCandidate);
    assertEquals(version.incGeneration(), newMembership.getSegmentVersion());
    assertEquals(membership.getPrimary(), newMembership.getPrimary());
    assertEquals(secondaryCandidate, newMembership.getSecondaryCandidate());
    assertNull(newMembership.getTempPrimary());
    assertArrayEquals(membership.getSecondaries().toArray(),
        newMembership.getSecondaries().toArray());
    assertArrayEquals(membership.getInactiveSecondaries().toArray(),
        newMembership.getInactiveSecondaries().toArray());
    assertArrayEquals(membership.getJoiningSecondaries().toArray(),
        newMembership.getJoiningSecondaries().toArray());

    serializeTest(membership);
    serializeTest(newMembership);
  }

  @Test
  public void removeSecondaryCandidate() throws Exception {
    int index = 0;
    SegmentVersion version = new SegmentVersion(0, 0);
    InstanceId primary = new InstanceId(index++);
    InstanceId tempPrimary = new InstanceId(index++);
    InstanceId secondaryCandidate = new InstanceId(index++);
    InstanceId secondary1 = new InstanceId(index++);
    InstanceId secondary2 = new InstanceId(index++);
    InstanceId arbiter1 = new InstanceId(index++);
    InstanceId arbiter2 = new InstanceId(index++);
    InstanceId inactiveSecondary1 = new InstanceId(index++);
    InstanceId inactiveSecondary2 = new InstanceId(index++);
    InstanceId joiningSecondary1 = new InstanceId(index++);
    InstanceId joiningSecondary2 = new InstanceId(index++);

    SegmentMembership membership = new SegmentMembership(version, primary, tempPrimary,
        Sets.newHashSet(secondary1, secondary2), Sets.newHashSet(arbiter1, arbiter2),
        Sets.newHashSet(inactiveSecondary1, inactiveSecondary2),
        Sets.newHashSet(joiningSecondary1, joiningSecondary2), secondaryCandidate, null);

    assertEquals(secondaryCandidate, membership.getSecondaryCandidate());

    SegmentMembership newMembership = membership.removeSecondaryCandidate(secondaryCandidate);

    assertEquals(version.incGeneration(), newMembership.getSegmentVersion());
    assertEquals(membership.getPrimary(), newMembership.getPrimary());
    assertNull(newMembership.getSecondaryCandidate());
    assertNull(newMembership.getTempPrimary());
    assertArrayEquals(membership.getSecondaries().toArray(),
        newMembership.getSecondaries().toArray());
    assertArrayEquals(membership.getInactiveSecondaries().toArray(),
        newMembership.getInactiveSecondaries().toArray());
    assertArrayEquals(membership.getJoiningSecondaries().toArray(),
        newMembership.getJoiningSecondaries().toArray());

    serializeTest(membership);
    serializeTest(newMembership);
  }

  @Test
  public void secondaryCandidateBecomesJoining() throws Exception {
    int index = 0;
    SegmentVersion version = new SegmentVersion(0, 0);
    InstanceId primary = new InstanceId(index++);
    InstanceId tempPrimary = new InstanceId(index++);
    InstanceId secondaryCandidate = new InstanceId(index++);
    InstanceId secondary1 = new InstanceId(index++);
    InstanceId secondary2 = new InstanceId(index++);
    InstanceId arbiter1 = new InstanceId(index++);
    InstanceId arbiter2 = new InstanceId(index++);
    InstanceId inactiveSecondary1 = new InstanceId(index++);
    InstanceId inactiveSecondary2 = new InstanceId(index++);
    InstanceId joiningSecondary1 = new InstanceId(index++);
    InstanceId joiningSecondary2 = new InstanceId(index++);

    SegmentMembership membership = new SegmentMembership(version, primary, tempPrimary,
        Sets.newHashSet(secondary1, secondary2), Sets.newHashSet(arbiter1, arbiter2),
        Sets.newHashSet(inactiveSecondary1, inactiveSecondary2),
        Sets.newHashSet(joiningSecondary1, joiningSecondary2), secondaryCandidate, null);

    SegmentMembership newMembership = membership
        .secondaryCandidateBecomesJoining(secondaryCandidate);

    assertEquals(version.incGeneration(), newMembership.getSegmentVersion());
    assertEquals(membership.getPrimary(), newMembership.getPrimary());
    assertNull(newMembership.getSecondaryCandidate());
    assertNull(newMembership.getTempPrimary());
    assertArrayEquals(membership.getSecondaries().toArray(),
        newMembership.getSecondaries().toArray());
    assertArrayEquals(membership.getInactiveSecondaries().toArray(),
        newMembership.getInactiveSecondaries().toArray());

    for (InstanceId oldJoining : membership.getJoiningSecondaries()) {
      assertTrue(newMembership.isJoiningSecondary(oldJoining));
    }
    assertTrue(newMembership.isJoiningSecondary(secondaryCandidate));

    serializeTest(membership);
    serializeTest(newMembership);
  }

  @Test
  public void testNewPrimaryChosen() {
    int index = 0;
    SegmentVersion version = new SegmentVersion(0, 0);
    InstanceId primary = new InstanceId(index++);
    InstanceId secondary1 = new InstanceId(index++);
    InstanceId secondary2 = new InstanceId(index++);

    SegmentMembership baseMembership = new SegmentMembership(version, primary,
        Sets.newHashSet(secondary1, secondary2));

    SegmentMembership newPrimaryChosenMembership = baseMembership
        .newPrimaryChosen(secondary1);

    assertEquals(secondary1, newPrimaryChosenMembership.getPrimary());
    assertTrue(newPrimaryChosenMembership.getSecondaries().contains(primary));
    assertTrue(newPrimaryChosenMembership.getSecondaries().contains(secondary2));

    SegmentMembership tempPrimaryMembership = baseMembership
        .secondaryBecomeTempPrimary(secondary1);

    assertEquals(secondary1, tempPrimaryMembership.getTempPrimary());

    SegmentMembership tpChosenMembership = tempPrimaryMembership
        .newPrimaryChosen(secondary1);

    assertTrue(tpChosenMembership.getInactiveSecondaries().contains(primary));
    assertTrue(tpChosenMembership.getSecondaries().contains(secondary2));

  }

  @Test
  public void secondaryCandidateBecomesSecondaryAndRemoveTheReplacee() throws Exception {
    int index = 0;
    SegmentVersion version = new SegmentVersion(0, 0);
    InstanceId primary = new InstanceId(index++);
    InstanceId tempPrimary = new InstanceId(index++);
    InstanceId secondaryCandidate = new InstanceId(index++);
    InstanceId secondary1 = new InstanceId(index++);
    InstanceId secondary2 = new InstanceId(index++);
    InstanceId arbiter1 = new InstanceId(index++);
    InstanceId arbiter2 = new InstanceId(index++);
    InstanceId inactiveSecondary1 = new InstanceId(index++);
    InstanceId inactiveSecondary2 = new InstanceId(index++);
    InstanceId joiningSecondary1 = new InstanceId(index++);
    InstanceId joiningSecondary2 = new InstanceId(index++);

    SegmentMembership membership = new SegmentMembership(version, primary, tempPrimary,
        Sets.newHashSet(secondary1, secondary2), Sets.newHashSet(arbiter1, arbiter2),
        Sets.newHashSet(inactiveSecondary1, inactiveSecondary2),
        Sets.newHashSet(joiningSecondary1, joiningSecondary2), secondaryCandidate, null);

    SegmentMembership newMembershipReplacingSecondary = membership
        .secondaryCandidateBecomesSecondaryAndRemoveTheReplacee(secondaryCandidate, secondary1);
    SegmentMembership newMembershipReplacingInactiveSecondary = membership
        .secondaryCandidateBecomesSecondaryAndRemoveTheReplacee(secondaryCandidate,
            inactiveSecondary1);
    SegmentMembership newMembershipReplacingJoiningSecondary = membership
        .secondaryCandidateBecomesSecondaryAndRemoveTheReplacee(secondaryCandidate,
            joiningSecondary1);

    for (SegmentMembership newMembership : new SegmentMembership[]{newMembershipReplacingSecondary,
        newMembershipReplacingInactiveSecondary, newMembershipReplacingJoiningSecondary}) {
      assertEquals(version.incGeneration(), newMembership.getSegmentVersion());
      assertEquals(membership.getPrimary(), newMembership.getPrimary());
      assertNull(newMembership.getSecondaryCandidate());
      assertNull(newMembership.getTempPrimary());
      assertTrue(newMembership.isSecondary(secondaryCandidate));
    }

    assertFalse(newMembershipReplacingInactiveSecondary.contain(inactiveSecondary1));
    assertTrue(newMembershipReplacingInactiveSecondary.getSecondaries()
        .containsAll(membership.getSecondaries()));
    assertArrayEquals(membership.getJoiningSecondaries().toArray(),
        newMembershipReplacingInactiveSecondary.getJoiningSecondaries().toArray());

    assertFalse(newMembershipReplacingJoiningSecondary.contain(joiningSecondary1));
    assertTrue(newMembershipReplacingJoiningSecondary.getSecondaries()
        .containsAll(membership.getSecondaries()));
    assertArrayEquals(membership.getInactiveSecondaries().toArray(),
        newMembershipReplacingJoiningSecondary.getInactiveSecondaries().toArray());

    assertFalse(newMembershipReplacingSecondary.contain(secondary1));
    assertArrayEquals(membership.getInactiveSecondaries().toArray(),
        newMembershipReplacingSecondary.getInactiveSecondaries().toArray());
    assertArrayEquals(membership.getJoiningSecondaries().toArray(),
        newMembershipReplacingSecondary.getJoiningSecondaries().toArray());

    serializeTest(membership);
    serializeTest(newMembershipReplacingInactiveSecondary);
    serializeTest(newMembershipReplacingJoiningSecondary);
    serializeTest(newMembershipReplacingSecondary);
  }

  @Test
  public void testPrimaryCandidate() throws Exception {
    int index = 0;
    SegmentVersion version = new SegmentVersion(0, 0);
    InstanceId primary = new InstanceId(index++);
    InstanceId tempPrimary = new InstanceId(index++);
    InstanceId secondary1 = new InstanceId(index++);
    InstanceId secondary2 = new InstanceId(index++);
    InstanceId primaryCandidate = secondary2;
    InstanceId arbiter1 = new InstanceId(index++);
    InstanceId arbiter2 = new InstanceId(index++);
    InstanceId inactiveSecondary1 = new InstanceId(index++);
    InstanceId inactiveSecondary2 = new InstanceId(index++);
    InstanceId joiningSecondary1 = new InstanceId(index++);
    InstanceId joiningSecondary2 = new InstanceId(index++);

    SegmentMembership membership = new SegmentMembership(version, primary, tempPrimary,
        Sets.newHashSet(secondary1, secondary2), Sets.newHashSet(arbiter1, arbiter2),
        Sets.newHashSet(inactiveSecondary1, inactiveSecondary2),
        Sets.newHashSet(joiningSecondary1, joiningSecondary2), null, primaryCandidate);

    SegmentMembership becomePrimary = membership.primaryCandidateBecomePrimary(primaryCandidate);
    // with the existing of a primary candidate, another secondary can't become primary candidate
    SegmentMembership secondaryBecomePc = membership.secondaryBecomePrimaryCandidate(secondary1);

    assertEquals(becomePrimary.getPrimary(), primaryCandidate);
    assertNull(secondaryBecomePc);

    // now secondary1 can be primary candidate because no primary candidate exists any more
    secondaryBecomePc = becomePrimary.secondaryBecomePrimaryCandidate(secondary1);
    assertEquals(secondaryBecomePc.getPrimaryCandidate(), secondary1);

    becomePrimary = secondaryBecomePc.primaryCandidateBecomePrimary(secondary1);
    assertEquals(becomePrimary.getPrimary(), secondary1);

    serializeTest(membership);
    serializeTest(becomePrimary);
    serializeTest(secondaryBecomePc);
  }

  public void serializeTest(SegmentMembership srcSegmentMembership) throws Exception {
    String bytes = srcSegmentMembership.serializeToObjectMapperContent();

    SegmentMembership dstSegmentMembership = SegmentMembership
        .deserializeFromObjectMapperContent(bytes);

    assertEquals(srcSegmentMembership, dstSegmentMembership);
  }
}
