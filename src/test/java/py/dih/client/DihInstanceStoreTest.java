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

package py.dih.client;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import py.common.RequestIdBuilder;
import py.common.struct.EndPoint;
import py.exception.GenericThriftClientFactoryException;
import py.instance.Instance;
import py.instance.InstanceId;
import py.instance.InstanceStatus;
import py.test.TestBase;

/**
 * A class contains some tests for {@link DihInstanceStore}.
 */
public class DihInstanceStoreTest extends TestBase {
  private static final Logger logger = LoggerFactory.getLogger(DihInstanceStoreTest.class);

  public static Random randomForBoolean = new Random(System.currentTimeMillis());
  public static Random randomForInteger = new Random(System.currentTimeMillis() + 100);

  @Mock
  private DihClientFactory dihClientFactory;
  private DihInstanceStore dihInstanceStore = DihInstanceStore.getSingleton();
  @Mock
  private DihServiceBlockingClientWrapper dihClientWrapper;

  @BeforeClass
  public void init() throws Exception {
    super.init();

    dihInstanceStore.setDihClientFactory(dihClientFactory);
    dihInstanceStore.setRefreshRate(1000);
    dihInstanceStore
        .setDihEndPoint(new EndPoint("localhost:" + (10000 + randomForInteger.nextInt())));
  }

  @Test
  public void chancesLost() throws Exception {
    Set<Instance> instances = new HashSet<>();
    for (int i = 0; i < 5; i++) {
      instances.add(randomGenInstance());
    }

    when(dihClientFactory.build(any(EndPoint.class), anyLong())).thenReturn(dihClientWrapper);
    when(dihClientWrapper.getInstanceAll()).thenReturn(instances);

    dihInstanceStore.init();

    logger.debug("Sleep 5 seconds ...");
    Thread.sleep(5000);
    Assert.assertEquals(instances, dihInstanceStore.getAll());

    when(dihClientFactory.build(any(EndPoint.class), anyLong())).thenThrow(
        new GenericThriftClientFactoryException());

    logger.debug("Sleep 5 seconds ...");
    Thread.sleep(5000);
    Assert.assertEquals(instances, dihInstanceStore.getAll());
  }

  public Instance randomGenInstance() {
    List<InstanceStatus> statuses = Arrays.asList(InstanceStatus.values());
    Collections.shuffle(statuses);

    Instance instance = new Instance(new InstanceId(RequestIdBuilder.get()),
        "DIH" + randomForInteger.nextInt(),
        statuses.get(0), new EndPoint("localhost:" + (10000 + randomForInteger.nextInt())));
    return instance;
  }

  @Test
  public void tryAllDihs() throws Exception {
    Set<Instance> instances = new HashSet<>();
    for (int i = 0; i < 3; i++) {
      instances.add(genInstance(i));
    }

    when(dihClientFactory.build(any(EndPoint.class), anyLong())).thenReturn(dihClientWrapper);
    when(dihClientWrapper.getInstanceAll()).thenReturn(instances);

    dihInstanceStore.init();

    logger.debug("Sleep 5 seconds ...");
    Thread.sleep(5000);
    Assert.assertEquals(instances, dihInstanceStore.getAll());

    Mockito.doAnswer(new Answer<DihServiceBlockingClientWrapper>() {
      @Override
      public DihServiceBlockingClientWrapper answer(InvocationOnMock invocation) throws Throwable {
        EndPoint endPoint = (EndPoint) (invocation.getArguments()[0]);
        logger.debug("DIH {}...", endPoint.toString());
        if (endPoint.toString().equals(String.valueOf("localhost:" + 10001))) {
          logger.debug("DIH 10001 ...");
          return dihClientWrapper;
        } else {
          logger.debug("DIH 10000 ...");
          throw new GenericThriftClientFactoryException();
        }
      }
    }).when(dihClientFactory).build(any(EndPoint.class), anyLong());

    when(dihClientWrapper.getInstanceAll()).thenReturn(instances);

    logger.debug("Sleep 5 seconds ...");
    Thread.sleep(5000);
    logger.debug("dihInstanceStore.getAll {} ", dihInstanceStore.getAll());

    Assert.assertEquals(instances, dihInstanceStore.getAll());

  }

  @Test
  public void tryAllDihsFail() throws Exception {
    Set<Instance> instances = new HashSet<>();
    for (int i = 0; i < 3; i++) {
      instances.add(genInstance(i));
    }

    when(dihClientFactory.build(any(EndPoint.class), anyLong())).thenReturn(dihClientWrapper);
    when(dihClientWrapper.getInstanceAll()).thenReturn(instances);

    dihInstanceStore.init();

    logger.debug("Sleep 5 seconds ...");
    Thread.sleep(5000);
    Assert.assertEquals(instances, dihInstanceStore.getAll());

    when(dihClientFactory.build(any(EndPoint.class), anyLong())).thenThrow(
        new GenericThriftClientFactoryException());

    logger.debug("Sleep 5 seconds ...");
    Thread.sleep(5000);
    Assert.assertEquals(instances, dihInstanceStore.getAll());

  }

  public Instance genInstance(int i) {
    Instance instance = new Instance(new InstanceId(RequestIdBuilder.get()), "DIH",
        InstanceStatus.HEALTHY, new EndPoint("localhost:" + (10000 + i)));
    return instance;
  }

}
