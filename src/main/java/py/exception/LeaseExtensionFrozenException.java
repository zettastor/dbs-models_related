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

package py.exception;

/**
 * This exception indicates that a secondary's lease can't be extended. The reason is that the
 * secondary can't be a secondary anymore for some reasons. However, it is bad to change its status
 * to Start immediately without waiting for its primary gets timed out.
 *
 * <p>Therefore, segment unit object can throw this exception when someone wants to extends its
 * lease
 *
 */
public class LeaseExtensionFrozenException extends Exception {
  private static final long serialVersionUID = 1L;

  public LeaseExtensionFrozenException() {
    super();
  }

  public LeaseExtensionFrozenException(String message) {
    super(message);
  }

  public LeaseExtensionFrozenException(String message, Throwable cause) {
    super(message, cause);
  }

  public LeaseExtensionFrozenException(Throwable cause) {
    super(cause);
  }
}
