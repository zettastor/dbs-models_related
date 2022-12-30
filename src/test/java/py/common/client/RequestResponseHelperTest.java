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

package py.common.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static py.RequestResponseHelper.buildVolumeSourceTypeFrom;

import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.Test;
import py.PbRequestResponseHelper;
import py.RequestResponseHelper;
import py.archive.segment.SegId;
import py.archive.segment.SegmentVersion;
import py.common.struct.EndPoint;
import py.instance.DummyInstanceStore;
import py.instance.Group;
import py.instance.Instance;
import py.instance.InstanceId;
import py.instance.InstanceStatus;
import py.instance.Location;
import py.instance.PortType;
import py.membership.SegmentMembership;
import py.proto.Broadcastlog;
import py.test.TestBase;
import py.thrift.share.SegmentMembershipThrift;
import py.thrift.share.VolumeSourceThrift;
import py.volume.VolumeMetadata;

public class RequestResponseHelperTest extends TestBase {
  private DummyInstanceStore instanceStore = new DummyInstanceStore();

  @Test
  public void testBuildEndPoints() {
    List<InstanceId> secondaries = new ArrayList<>();
    InstanceId id1 = new InstanceId(1L);
    InstanceId id2 = new InstanceId(2L);
    InstanceId id3 = new InstanceId(3L);

    secondaries.add(id2);
    secondaries.add(id3);

    Set<Instance> allInstances = new HashSet<>();

    Instance instance = new Instance(id1, new Group(1), new Location("c", "1"), "aaaa",
        InstanceStatus.HEALTHY);
    instance.putEndPointByServiceName(PortType.CONTROL, new EndPoint("111", 1));
    allInstances.add(instance);

    instance = new Instance(id2, new Group(1),
        new Location("c", "2"), "aaaa", InstanceStatus.HEALTHY);
    instance.putEndPointByServiceName(PortType.CONTROL, new EndPoint("111", 2));
    allInstances.add(instance);

    instance = new Instance(id3, new Group(1),
        new Location("c", "3"), "aaaa", InstanceStatus.HEALTHY);
    instance.putEndPointByServiceName(PortType.CONTROL, new EndPoint("111", 3));
    allInstances.add(instance);
    instanceStore.setInstances(allInstances);

    SegmentMembership membership = new SegmentMembership(id1, secondaries);
    List<EndPoint> endPoints = RequestResponseHelper
        .buildEndPoints(instanceStore, membership, true);
    assertEquals(3, endPoints.size());
    endPoints = RequestResponseHelper
        .buildEndPoints(instanceStore, membership, true, new InstanceId(6));
    assertEquals(3, endPoints.size());
    endPoints = RequestResponseHelper.buildEndPoints(instanceStore, membership, true);
    assertEquals(3, endPoints.size());

    endPoints = RequestResponseHelper.buildEndPoints(instanceStore, membership, true);
    assertEquals(3, endPoints.size());

    endPoints = RequestResponseHelper
        .buildEndPoints(instanceStore, membership, true, new InstanceId(1));
    assertEquals(2, endPoints.size());

    endPoints = RequestResponseHelper
        .buildEndPoints(instanceStore, membership, true, new InstanceId(2));
    assertEquals(2, endPoints.size());

    endPoints = RequestResponseHelper
        .buildEndPoints(instanceStore, membership, true, new InstanceId(2));
    assertEquals(2, endPoints.size());

    endPoints = RequestResponseHelper
        .buildEndPoints(instanceStore, membership, true, new InstanceId(3));
    assertEquals(2, endPoints.size());

    endPoints = RequestResponseHelper
        .buildEndPoints(instanceStore, membership, true, new InstanceId(1),
            new InstanceId(3));
    assertEquals(1, endPoints.size());

    endPoints = RequestResponseHelper
        .buildEndPoints(instanceStore, membership, true, new InstanceId(2),
            new InstanceId(1), new InstanceId(3));
    assertEquals(0, endPoints.size());

  }

  @Test
  public void testConvertMembershipFromAndToThrift() throws Exception {
    InstanceId[] instanceIds = new InstanceId[10];
    for (int i = 0; i < 10; i++) {
      instanceIds[i] = new InstanceId(i + 1);
    }

    SegId segId = new SegId(1L, 1);

    List<InstanceId> secondaries = Arrays.asList(instanceIds[1], instanceIds[2]);
    SegmentMembership membership = new SegmentMembership(instanceIds[0], secondaries);

    SegmentMembershipThrift membershipThrfit = RequestResponseHelper
        .buildThriftMembershipFrom(segId, membership);
    assertEquals(segId,
        RequestResponseHelper.buildSegmentMembershipFrom(membershipThrfit).getFirst());
    assertEquals(membership,
        RequestResponseHelper.buildSegmentMembershipFrom(membershipThrfit).getSecond());

    List<InstanceId> arbiter = Arrays.asList(instanceIds[3], instanceIds[4]);
    membership = new SegmentMembership(instanceIds[0], secondaries, arbiter);

    membershipThrfit = RequestResponseHelper.buildThriftMembershipFrom(segId, membership);
    assertEquals(membership,
        RequestResponseHelper.buildSegmentMembershipFrom(membershipThrfit).getSecond());

    List<InstanceId> inactiveSecondaries = Arrays.asList(instanceIds[7], instanceIds[8]);
    membership = new SegmentMembership(new SegmentVersion(1, 1), instanceIds[0], secondaries,
        arbiter,
        inactiveSecondaries, null);
    membershipThrfit = RequestResponseHelper.buildThriftMembershipFrom(segId, membership);
    assertEquals(membership,
        RequestResponseHelper.buildSegmentMembershipFrom(membershipThrfit).getSecond());

    List<InstanceId> joiningSecondaries = Arrays.asList(instanceIds[6], instanceIds[5]);
    membership = new SegmentMembership(new SegmentVersion(1, 1), instanceIds[0], secondaries,
        arbiter,
        inactiveSecondaries, joiningSecondaries);

    membershipThrfit = RequestResponseHelper.buildThriftMembershipFrom(segId, membership);
    assertEquals(membership,
        RequestResponseHelper.buildSegmentMembershipFrom(membershipThrfit).getSecond());

    // test lease
    membershipThrfit = RequestResponseHelper.buildThriftMembershipFrom(segId, membership, 100);
    assertEquals(100, membershipThrfit.getLease());

  }

  @Test
  public void testBuildThriftVolumeSource() {
    // test null volume source type
    VolumeMetadata.VolumeSourceType sourceType = null;
    VolumeSourceThrift volumeSourceThrift = RequestResponseHelper
        .buildThriftVolumeSource(sourceType);
    assertNull(volumeSourceThrift);
    sourceType = VolumeMetadata.VolumeSourceType.CREATE_VOLUME;
    volumeSourceThrift = RequestResponseHelper.buildThriftVolumeSource(sourceType);
    assertEquals(VolumeSourceThrift.CREATE, volumeSourceThrift);
  }

  @Test
  public void testBuildVolumeSourceTypeFromThrift() {
    // test null volume source thrift type
    VolumeSourceThrift volumeSourceThrift = null;
    VolumeMetadata.VolumeSourceType sourceType = buildVolumeSourceTypeFrom(volumeSourceThrift);
    assertNull(sourceType);
    volumeSourceThrift = VolumeSourceThrift.CREATE;
    sourceType = buildVolumeSourceTypeFrom(volumeSourceThrift);
    assertEquals(VolumeMetadata.VolumeSourceType.CREATE_VOLUME, sourceType);
  }

  @Test
  public void testBuildMembershipWithTempPrimary() throws Exception {
    Set<InstanceId> secondaries = new HashSet<>();
    secondaries.add(new InstanceId(2));
    SegmentMembership membership = new SegmentMembership(new SegmentVersion(1, 1),
        new InstanceId(1),
        new InstanceId(2), secondaries, null, null, null, null, null);
    SegmentMembershipThrift thriftMembership = RequestResponseHelper
        .buildThriftMembershipFrom(1, 1, membership);
    assertEquals(2, thriftMembership.getTempPrimary());
  }

  @Test
  public void testBuildMembershipFromThrift() throws Exception {
    int index = 0;
    InstanceId primary = new InstanceId(index++);
    InstanceId tempPrimary = new InstanceId(index++);
    InstanceId secondaryCandidate = new InstanceId(index++);
    InstanceId primaryCandidate = new InstanceId(index++);
    InstanceId secondary1 = new InstanceId(index++);
    InstanceId secondary2 = new InstanceId(index++);
    InstanceId arbiter1 = new InstanceId(index++);
    InstanceId arbiter2 = new InstanceId(index++);
    InstanceId inactiveSecondary1 = new InstanceId(index++);
    InstanceId inactiveSecondary2 = new InstanceId(index++);
    InstanceId joiningSecondary1 = new InstanceId(index++);
    InstanceId joiningSecondary2 = new InstanceId(index);

    SegmentMembership membership = new SegmentMembership(new SegmentVersion(0, 0), primary,
        tempPrimary,
        Sets.newHashSet(secondary1, secondary2), Sets.newHashSet(arbiter1, arbiter2),
        Sets.newHashSet(inactiveSecondary1, inactiveSecondary2),
        Sets.newHashSet(joiningSecondary1, joiningSecondary2), secondaryCandidate,
        primaryCandidate);

    SegmentMembershipThrift thriftMembership = RequestResponseHelper
        .buildThriftMembershipFrom(new SegId(0, 0), membership);
    SegmentMembership convertedFromThrift = RequestResponseHelper
        .buildSegmentMembershipFrom(thriftMembership)
        .getSecond();
    assertEquals(membership, convertedFromThrift);

    Broadcastlog.PbMembership pbMembership = PbRequestResponseHelper
        .buildPbMembershipFrom(membership);
    SegmentMembership convertedFromPb = PbRequestResponseHelper.buildMembershipFrom(pbMembership);
    assertEquals(membership, convertedFromPb);

  }
}