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

package py.archive.page;

import org.apache.commons.lang.NotImplementedException;

public enum PageAddressType {
  ORIGINAL {
    @Override
    public boolean isOriginalPageAddress() {
      return true;
    }
  }, // for original data page
  SHADOW {
    @Override
    public boolean isOriginalPageAddress() {
      return false;
    }
  }; // for shadow data page

  public boolean isOriginalPageAddress() {
    throw new NotImplementedException();
  }
}
