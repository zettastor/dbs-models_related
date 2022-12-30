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

package py.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import py.membership.MemberIoStatus;

public class MemberIoStatusTest extends TestBase {
  @Test
  public void testSameRole() {
    assertTrue(MemberIoStatus.Primary.sameRole(MemberIoStatus.PrimaryDown));
    assertTrue(MemberIoStatus.Primary.sameRole(MemberIoStatus.UnstablePrimary));

    assertTrue(MemberIoStatus.Secondary.sameRole(MemberIoStatus.SecondaryDown));
    assertTrue(MemberIoStatus.Secondary.sameRole(MemberIoStatus.SecondaryReadDown));
    assertTrue(MemberIoStatus.Secondary.sameRole(MemberIoStatus.TempPrimary));

    assertTrue(MemberIoStatus.JoiningSecondary.sameRole(MemberIoStatus.JoiningSecondaryDown));

    assertTrue(MemberIoStatus.Arbiter.sameRole(MemberIoStatus.ArbiterDown));

    assertTrue(!MemberIoStatus.Primary.sameRole(MemberIoStatus.TempPrimary));
    assertTrue(!MemberIoStatus.Primary.sameRole(MemberIoStatus.SecondaryReadDown));
    assertTrue(!MemberIoStatus.Primary.sameRole(MemberIoStatus.JoiningSecondaryDown));

    assertTrue(!MemberIoStatus.Secondary.sameRole(MemberIoStatus.PrimaryDown));
    assertTrue(!MemberIoStatus.Secondary.sameRole(MemberIoStatus.UnstablePrimary));

    assertTrue(!MemberIoStatus.JoiningSecondary.sameRole(MemberIoStatus.ArbiterDown));

    assertTrue(!MemberIoStatus.Arbiter.sameRole(MemberIoStatus.SecondaryDown));

    boolean caughtException = false;
    try {
      assertTrue(MemberIoStatus.InactiveSecondary.sameRole(MemberIoStatus.PrimaryDown));
    } catch (Exception e) {
      caughtException = true;
    }

    assertTrue(caughtException);

    caughtException = false;
    try {
      assertTrue(MemberIoStatus.ExternalMember.sameRole(MemberIoStatus.PrimaryDown));
    } catch (Exception e) {
      caughtException = true;
    }

    assertTrue(caughtException);
  }

  @Test
  public void testCanMoveTo() {
    assertTrue(MemberIoStatus.Primary.canMoveTo(MemberIoStatus.Primary));
    assertTrue(MemberIoStatus.Primary.canMoveTo(MemberIoStatus.PrimaryDown));
    assertTrue(MemberIoStatus.Primary.canMoveTo(MemberIoStatus.UnstablePrimary));
    assertTrue(MemberIoStatus.UnstablePrimary.canMoveTo(MemberIoStatus.Primary));

    assertTrue(!MemberIoStatus.PrimaryDown.canMoveTo(MemberIoStatus.Primary));
    assertTrue(!MemberIoStatus.Primary.canMoveTo(MemberIoStatus.SecondaryDown));

    assertTrue(MemberIoStatus.Secondary.canMoveTo(MemberIoStatus.Secondary));
    assertTrue(MemberIoStatus.Secondary.canMoveTo(MemberIoStatus.SecondaryDown));
    assertTrue(MemberIoStatus.Secondary.canMoveTo(MemberIoStatus.SecondaryReadDown));
    assertTrue(MemberIoStatus.Secondary.canMoveTo(MemberIoStatus.TempPrimary));
    assertTrue(MemberIoStatus.TempPrimary.canMoveTo(MemberIoStatus.Secondary));

    assertTrue(!MemberIoStatus.SecondaryDown.canMoveTo(MemberIoStatus.Secondary));
    assertTrue(!MemberIoStatus.SecondaryReadDown.canMoveTo(MemberIoStatus.Secondary));
    assertTrue(!MemberIoStatus.Secondary.canMoveTo(MemberIoStatus.PrimaryDown));

    assertTrue(MemberIoStatus.JoiningSecondary.canMoveTo(MemberIoStatus.JoiningSecondaryDown));

    assertTrue(!MemberIoStatus.JoiningSecondary.canMoveTo(MemberIoStatus.ArbiterDown));
    assertTrue(!MemberIoStatus.JoiningSecondaryDown.canMoveTo(MemberIoStatus.JoiningSecondary));

    assertTrue(MemberIoStatus.Arbiter.canMoveTo(MemberIoStatus.ArbiterDown));

    assertTrue(!MemberIoStatus.ArbiterDown.canMoveTo(MemberIoStatus.Arbiter));
    assertTrue(!MemberIoStatus.Arbiter.canMoveTo(MemberIoStatus.JoiningSecondaryDown));

  }

  @Test
  public void testMergeMemberIoStatus() {
    assertEquals(MemberIoStatus.Primary,
        MemberIoStatus.Primary.mergeMemberIoStatus(MemberIoStatus.Primary));
    assertEquals(MemberIoStatus.PrimaryDown,
        MemberIoStatus.Primary.mergeMemberIoStatus(MemberIoStatus.PrimaryDown));
    assertEquals(MemberIoStatus.PrimaryDown,
        MemberIoStatus.PrimaryDown.mergeMemberIoStatus(MemberIoStatus.Primary));
    assertEquals(MemberIoStatus.Primary,
        MemberIoStatus.Primary.mergeMemberIoStatus(MemberIoStatus.UnstablePrimary));

    assertEquals(MemberIoStatus.Primary,
        MemberIoStatus.Primary.mergeMemberIoStatus(MemberIoStatus.TempPrimary));

    assertEquals(MemberIoStatus.Secondary,
        MemberIoStatus.Secondary.mergeMemberIoStatus(MemberIoStatus.Secondary));
    assertEquals(MemberIoStatus.SecondaryDown,
        MemberIoStatus.Secondary.mergeMemberIoStatus(MemberIoStatus.SecondaryDown));
    assertEquals(MemberIoStatus.SecondaryReadDown,
        MemberIoStatus.Secondary.mergeMemberIoStatus(MemberIoStatus.SecondaryReadDown));
    assertEquals(MemberIoStatus.SecondaryReadDown,
        MemberIoStatus.SecondaryReadDown.mergeMemberIoStatus(MemberIoStatus.Secondary));

    assertEquals(MemberIoStatus.Secondary,
        MemberIoStatus.Secondary.mergeMemberIoStatus(MemberIoStatus.UnstablePrimary));

    assertEquals(MemberIoStatus.JoiningSecondary,
        MemberIoStatus.JoiningSecondary.mergeMemberIoStatus(MemberIoStatus.JoiningSecondary));
    assertEquals(MemberIoStatus.JoiningSecondaryDown,
        MemberIoStatus.JoiningSecondary.mergeMemberIoStatus(MemberIoStatus.JoiningSecondaryDown));
    assertEquals(MemberIoStatus.JoiningSecondaryDown,
        MemberIoStatus.JoiningSecondaryDown.mergeMemberIoStatus(MemberIoStatus.JoiningSecondary));

    assertEquals(MemberIoStatus.JoiningSecondary,
        MemberIoStatus.JoiningSecondary.mergeMemberIoStatus(MemberIoStatus.ArbiterDown));

    assertEquals(MemberIoStatus.Arbiter,
        MemberIoStatus.Arbiter.mergeMemberIoStatus(MemberIoStatus.Arbiter));
    assertEquals(MemberIoStatus.ArbiterDown,
        MemberIoStatus.Arbiter.mergeMemberIoStatus(MemberIoStatus.ArbiterDown));
    assertEquals(MemberIoStatus.ArbiterDown,
        MemberIoStatus.ArbiterDown.mergeMemberIoStatus(MemberIoStatus.Arbiter));

    assertEquals(MemberIoStatus.Arbiter,
        MemberIoStatus.Arbiter.mergeMemberIoStatus(MemberIoStatus.JoiningSecondaryDown));
  }

  @Test
  public void testDownStatus() {
    assertTrue(!MemberIoStatus.Primary.isReadDown());
    assertTrue(!MemberIoStatus.Primary.isWriteDown());
    assertTrue(!MemberIoStatus.Primary.isDown());

    assertTrue(!MemberIoStatus.Secondary.isReadDown());
    assertTrue(!MemberIoStatus.Secondary.isWriteDown());
    assertTrue(!MemberIoStatus.Secondary.isDown());

    assertTrue(!MemberIoStatus.JoiningSecondary.isReadDown());
    assertTrue(!MemberIoStatus.JoiningSecondary.isWriteDown());
    assertTrue(!MemberIoStatus.JoiningSecondary.isDown());

    assertTrue(!MemberIoStatus.Arbiter.isReadDown());
    assertTrue(!MemberIoStatus.Arbiter.isWriteDown());
    assertTrue(!MemberIoStatus.Arbiter.isDown());

    assertTrue(MemberIoStatus.PrimaryDown.isReadDown());
    assertTrue(MemberIoStatus.PrimaryDown.isWriteDown());
    assertTrue(MemberIoStatus.PrimaryDown.isDown());

    assertTrue(MemberIoStatus.SecondaryDown.isReadDown());
    assertTrue(MemberIoStatus.SecondaryDown.isWriteDown());
    assertTrue(MemberIoStatus.SecondaryDown.isDown());

    assertTrue(MemberIoStatus.JoiningSecondaryDown.isReadDown());
    assertTrue(MemberIoStatus.JoiningSecondaryDown.isWriteDown());
    assertTrue(MemberIoStatus.JoiningSecondaryDown.isDown());

    assertTrue(MemberIoStatus.ArbiterDown.isReadDown());
    assertTrue(MemberIoStatus.ArbiterDown.isWriteDown());
    assertTrue(MemberIoStatus.ArbiterDown.isDown());

    assertTrue(MemberIoStatus.InactiveSecondary.isReadDown());
    assertTrue(MemberIoStatus.InactiveSecondary.isWriteDown());
    assertTrue(MemberIoStatus.InactiveSecondary.isDown());

    assertTrue(MemberIoStatus.ExternalMember.isReadDown());
    assertTrue(MemberIoStatus.ExternalMember.isWriteDown());
    assertTrue(MemberIoStatus.ExternalMember.isDown());

    assertTrue(!MemberIoStatus.TempPrimary.isReadDown());
    assertTrue(!MemberIoStatus.TempPrimary.isWriteDown());
    assertTrue(!MemberIoStatus.TempPrimary.isDown());

    assertTrue(!MemberIoStatus.UnstablePrimary.isReadDown());
    assertTrue(!MemberIoStatus.UnstablePrimary.isWriteDown());
    assertTrue(!MemberIoStatus.UnstablePrimary.isDown());

    assertTrue(MemberIoStatus.SecondaryReadDown.isReadDown());
    assertTrue(!MemberIoStatus.SecondaryReadDown.isWriteDown());
    assertTrue(!MemberIoStatus.SecondaryReadDown.isDown());

  }

}
