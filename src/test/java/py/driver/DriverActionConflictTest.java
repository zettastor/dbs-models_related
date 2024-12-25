

package py.driver;

import org.junit.Assert;
import org.junit.Test;
import py.test.TestBase;

/**
 * Test different driver action conflict or not.
 */
public class DriverActionConflictTest extends TestBase {
  @Override
  public void init() throws Exception {
    super.init();
  }

  @Test
  public void testDriverActionConflict() {
    Assert.assertTrue(DriverAction.CHECK_MIGRATING.isConflictWith(DriverAction.CHANGE_VOLUME));
    Assert.assertTrue(DriverAction.CHECK_MIGRATING.isConflictWith(DriverAction.CHECK_MIGRATING));
    Assert.assertFalse(DriverAction.CHANGE_VOLUME.isConflictWith(DriverAction.CHECK_SERVER));
  }
}
