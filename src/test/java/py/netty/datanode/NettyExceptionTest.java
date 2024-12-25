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

package py.netty.datanode;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;
import py.app.context.AppContext;
import py.archive.segment.SegId;
import py.archive.segment.SegmentUnitStatus;
import py.archive.segment.SegmentVersion;
import py.instance.InstanceId;
import py.membership.SegmentMembership;
import py.netty.exception.AbstractNettyException;
import py.netty.exception.ExceptionType;
import py.netty.exception.MembershipVersionLowerException;
import py.netty.exception.NotPrimaryException;
import py.netty.exception.NotSecondaryException;
import py.netty.exception.SegmentNotFoundException;
import py.netty.exception.ServerOverLoadedException;
import py.netty.exception.ServerProcessException;
import py.test.TestBase;

public class NettyExceptionTest extends TestBase {
  private SegId segId = new SegId(1, 2);
  private AppContext appContext = mock(AppContext.class);
  private InstanceId instanceId = new InstanceId(3);
  private long volumeSize = 512;

  public NettyExceptionTest() {
    when(appContext.getInstanceId()).thenReturn(instanceId);
  }

  @Test
  public void segmentNotFoundExcetpion() throws Exception {
    SegmentNotFoundException e1 = NettyExceptionHelper
        .buildSegmentNotFoundException(segId, appContext);
    ByteBuf buffer = Unpooled.wrappedBuffer(new byte[e1.getSize()]);
    e1.toBuffer(buffer.duplicate().clear());

    SegmentNotFoundException e2 = (SegmentNotFoundException) AbstractNettyException.parse(buffer);

    assertEquals(e1.getInstanceId(), e2.getInstanceId());
    assertEquals(e1.getExceptionType(), e2.getExceptionType());
    assertEquals(e1.getExceptionType(), ExceptionType.SEGMENTNOTFOUND);
    assertEquals(e1.getSegIndex(), e2.getSegIndex());
    assertEquals(e1.getVolumeId(), e2.getVolumeId());
  }

  @Test
  public void notPrimaryException() throws Exception {
    SegmentMembership membership = generateMembership();
    NotPrimaryException e1 = NettyExceptionHelper
        .buildNotPrimaryException(segId, SegmentUnitStatus.PrePrimary,
            membership);
    ByteBuf buffer = Unpooled.wrappedBuffer(new byte[e1.getSize()]);
    e1.toBuffer(buffer.duplicate().clear());

    NotPrimaryException e2 = (NotPrimaryException) AbstractNettyException.parse(buffer);

    assertEquals(e1.getStatus(), e2.getStatus());
    assertEquals(e1.getExceptionType(), e2.getExceptionType());
    assertEquals(e1.getExceptionType(), ExceptionType.NOTPRIMARY);
    assertEquals(e1.getSegIndex(), e2.getSegIndex());
    assertEquals(e1.getVolumeId(), e2.getVolumeId());
    assertTrue(membership.equals(NettyExceptionHelper.getMembershipFromBuffer(e1.getMembership())));
    assertTrue(membership.equals(NettyExceptionHelper.getMembershipFromBuffer(e2.getMembership())));
  }

  @Test
  public void notSecondaryException() throws Exception {
    SegmentMembership membership = generateMembership();
    NotSecondaryException e1 = NettyExceptionHelper
        .buildNotSecondaryException(segId, SegmentUnitStatus.PrePrimary,
            membership);
    ByteBuf buffer = Unpooled.wrappedBuffer(new byte[e1.getSize()]);
    e1.toBuffer(buffer.duplicate().clear());

    NotSecondaryException e2 = (NotSecondaryException) AbstractNettyException.parse(buffer);

    assertEquals(e1.getStatus(), e2.getStatus());
    assertEquals(e1.getExceptionType(), e2.getExceptionType());
    assertEquals(e1.getExceptionType(), ExceptionType.NOTSECONDARY);
    assertEquals(e1.getSegIndex(), e2.getSegIndex());
    assertEquals(e1.getVolumeId(), e2.getVolumeId());
    assertTrue(membership.equals(NettyExceptionHelper.getMembershipFromBuffer(e1.getMembership())));
    assertTrue(membership.equals(NettyExceptionHelper.getMembershipFromBuffer(e2.getMembership())));
  }

  @Test
  public void membershipVersionLowerException() throws Exception {
    SegmentMembership membership = generateMembership();
    MembershipVersionLowerException e1 = NettyExceptionHelper
        .buildMembershipVersionLowerException(segId,
            membership);
    ByteBuf buffer = Unpooled.wrappedBuffer(new byte[e1.getSize()]);
    e1.toBuffer(buffer.duplicate().clear());

    MembershipVersionLowerException e2 = (MembershipVersionLowerException) AbstractNettyException
        .parse(buffer);

    assertEquals(e1.getExceptionType(), e2.getExceptionType());
    assertEquals(e1.getExceptionType(), ExceptionType.MEMBERSHIPLOWER);
    assertEquals(e1.getSegIndex(), e2.getSegIndex());
    assertEquals(e1.getVolumeId(), e2.getVolumeId());
    assertTrue(membership.equals(NettyExceptionHelper.getMembershipFromBuffer(e1.getMembership())));
    assertTrue(membership.equals(NettyExceptionHelper.getMembershipFromBuffer(e2.getMembership())));
  }

  @Test
  public void serverProcessException() throws Exception {
    ServerProcessException e1 = NettyExceptionHelper
        .buildServerProcessException("pengyun is the best one in cloud compute area");
    ByteBuf buffer = Unpooled.wrappedBuffer(new byte[e1.getSize()]);
    e1.toBuffer(buffer.duplicate().clear());
    ServerProcessException e2 = (ServerProcessException) AbstractNettyException.parse(buffer);
    assertTrue(e1.getMessage().equals(e2.getMessage()));

    ServerProcessException e3 = NettyExceptionHelper.buildServerProcessException(
        new NullPointerException("pengyun is the best one in cloud compute area"));
    buffer = Unpooled.wrappedBuffer(new byte[e3.getSize()]);
    e3.toBuffer(buffer.duplicate().clear());
    ServerProcessException e4 = (ServerProcessException) AbstractNettyException.parse(buffer);
    assertTrue(e3.getMessage().equals(e4.getMessage()));
    logger.warn("exception: {} ", e4.getMessage(), e4);
  }

  @Test
  public void serverOverLoadedException() throws Exception {
    ServerOverLoadedException e1 = NettyExceptionHelper
        .buildServerOverLoadedException(11, 2);
    ByteBuf buffer = Unpooled.wrappedBuffer(new byte[e1.getSize()]);
    e1.toBuffer(buffer.duplicate().clear());
    ServerOverLoadedException e2 = (ServerOverLoadedException) AbstractNettyException.parse(buffer);
    assertEquals(e1.getPendingRequests(), e2.getPendingRequests());
    assertEquals(e1.getQueueLength(), e2.getQueueLength());
  }

  private SegmentMembership generateMembership() {
    List<InstanceId> secondaries = new ArrayList<>();
    secondaries.add(new InstanceId(2L));
    secondaries.add(new InstanceId(3L));
    return new SegmentMembership(new SegmentVersion(10, 11), new InstanceId(1), secondaries);
  }
}
