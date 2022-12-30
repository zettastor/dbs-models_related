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

package py.volume.limitation;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang.Validate;
import org.junit.Test;
import py.RequestResponseHelper;
import py.exception.LimitTypeDismatchException;
import py.exception.TimeSpanAlreadyExistException;
import py.icshare.qos.IoLimitScheduler;
import py.io.qos.IoLimitManager;
import py.io.qos.IoLimitation;
import py.io.qos.IoLimitationEntry;
import py.periodic.UnableToStartException;
import py.test.TestBase;

public class IoLimitTest extends TestBase {
  @Test
  public void testIoLimitSchedule()
      throws LimitTypeDismatchException, TimeSpanAlreadyExistException,
      InterruptedException {
    DummyIoLimitManager manager = new DummyIoLimitManager();
    IoLimitScheduler scheduler = new IoLimitScheduler(manager);
    try {
      LocalTime startTime1 = LocalTime.now().plusSeconds(5);
      LocalTime endTime1 = LocalTime.now().plusSeconds(10);

      LocalTime startTime2 = LocalTime.now().plusSeconds(10);
      LocalTime endTime2 = LocalTime.now().plusSeconds(15);

      LocalTime startTime3 = LocalTime.now().plusSeconds(20);
      LocalTime endTime3 = LocalTime.now().plusSeconds(25);

      IoLimitationEntry limit1 = new IoLimitationEntry(1L, 100, 10, 1000, 100,
          LocalTime.now().plusSeconds(5), LocalTime
          .now().plusSeconds(10));
      IoLimitationEntry limit2 = new IoLimitationEntry(2L, 200, 20, 2000, 200,
          LocalTime.now().plusSeconds(10), LocalTime
          .now().plusSeconds(15));
      IoLimitationEntry limit3 = new IoLimitationEntry(3L, 300, 30, 3000, 300,
          LocalTime.now().plusSeconds(20), LocalTime
          .now().plusSeconds(25));

      List<IoLimitationEntry> entries = new ArrayList<>();
      entries.add(limit1);
      entries.add(limit2);
      entries.add(limit3);

      IoLimitation ioLimitation = new IoLimitation();
      ioLimitation.setEntries(entries);

      scheduler.addOrModifyIoLimitation(ioLimitation);

      scheduler.open();

      while (true) {
        logger.debug("check now {}", LocalTime.now());
        if (inTimeSpan(startTime1, endTime1)) {
          Validate.isTrue(manager.getIoLimitationEntry() == limit1);
        } else if (inTimeSpan(startTime2, endTime2)) {
          Validate.isTrue(manager.getIoLimitationEntry() == limit2);
        } else if (inTimeSpan(startTime3, endTime3)) {
          Validate.isTrue(manager.getIoLimitationEntry() == limit3);
        } else if (LocalTime.now().isAfter(endTime3)) {
          Validate.isTrue(!manager.isOpen());
          break;
        } else {
          Validate.isTrue(!manager.isOpen());
        }
        Thread.sleep(1000);
      }

    } catch (Exception e1) {
      e1.printStackTrace();
    }
  }

  @Test
  public void testJudgeIoLimitationTimeInterleaving() {
    LocalTime startTime1 = LocalTime.now().plusSeconds(5);
    LocalTime endTime1 = LocalTime.now().plusSeconds(10);

    LocalTime startTime2 = LocalTime.now().plusSeconds(11);

    IoLimitationEntry entry1 = new IoLimitationEntry();
    entry1.setStartTime(startTime1.toString());
    entry1.setEndTime(endTime1.toString());

    IoLimitationEntry entry2 = new IoLimitationEntry();
    entry2.setStartTime(startTime2.toString());
    LocalTime endTime2 = LocalTime.now().plusSeconds(15);
    entry2.setEndTime(endTime2.toString());

    IoLimitationEntry endTimeBeforeStartTimeEntry = new IoLimitationEntry();
    LocalTime endTime3 = LocalTime.now().plusSeconds(25);
    endTimeBeforeStartTimeEntry.setStartTime(endTime3.toString());

    LocalTime startTime3 = LocalTime.now().plusSeconds(20);
    endTimeBeforeStartTimeEntry.setEndTime(startTime3.toString());

    IoLimitation limitation = new IoLimitation();
    limitation.setLimitType(IoLimitation.LimitType.Dynamic);
    List<IoLimitationEntry> okEntries = new ArrayList<>();
    okEntries.add(entry1);
    okEntries.add(entry2);
    limitation.setEntries(okEntries);
    assertFalse(RequestResponseHelper.judgeDynamicIoLimitationTimeInterleaving(limitation));

    List<IoLimitationEntry> entriesWithSameStartTime = new ArrayList<>();
    entriesWithSameStartTime.add(entry1);
    entriesWithSameStartTime.add(entry1);
    limitation.setEntries(entriesWithSameStartTime);
    assertTrue(RequestResponseHelper.judgeDynamicIoLimitationTimeInterleaving(limitation));

    List<IoLimitationEntry> entriesWithEndTimeBeforeStartTime = new ArrayList<>();
    entriesWithEndTimeBeforeStartTime.add(endTimeBeforeStartTimeEntry);
    limitation.setEntries(entriesWithEndTimeBeforeStartTime);
    assertTrue(RequestResponseHelper.judgeDynamicIoLimitationTimeInterleaving(limitation));
  }

  private boolean inTimeSpan(LocalTime startTime, LocalTime endTime) {
    LocalTime now = LocalTime.now();
    if (startTime.isAfter(endTime)) {
      return false;
    }
    if (now.isBefore(startTime) || now.isAfter(endTime)) {
      return false;
    }
    return true;
  }

  class DummyIoLimitManager implements IoLimitManager {
    private IoController thread;

    @Override
    public void updateLimitationsAndOpen(IoLimitationEntry ioLimitationEntry)
        throws UnableToStartException {
      if (thread != null) {
        thread.quit();
      }
      thread = new IoController(ioLimitationEntry);
      thread.start();
    }

    @Override
    public void close() {
      if (thread != null) {
        thread.quit();
      }
    }

    @Override
    public boolean isOpen() {
      if (thread == null) {
        return false;
      }
      return thread.isAlive();
    }

    @Override
    public IoLimitationEntry getIoLimitationEntry() {
      return thread.getIoLimitationEntry();
    }

    @Override
    public void tryGettingAnIo() {
      // TODO Auto-generated method stub
    }

    @Override
    public void tryThroughput(long size) {
      // TODO Auto-generated method stub
    }

    @Override
    public void slowDownExceptFor(long volumeId, int level) {
      // TODO Auto-generated method stub

    }

    @Override
    public void resetSlowLevel(long volumeId) {
      // TODO Auto-generated method stub

    }

    class IoController extends Thread {
      private IoLimitationEntry ioLimitationEntry;
      private boolean quit;

      public IoController(IoLimitationEntry ioLimitationEntry) {
        this.ioLimitationEntry = ioLimitationEntry;
      }

      @Override
      public void run() {
        while (!quit) {
          System.out.println(ioLimitationEntry);
          try {
            Thread.sleep(1000);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
      }

      public void quit() {
        this.quit = true;
      }

      public IoLimitationEntry getIoLimitationEntry() {
        return ioLimitationEntry;
      }

    }

  }

}
