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

package py.icshare;

import java.util.List;

public interface InstanceMaintenanceDbStore {
  void save(InstanceMaintenanceInformation instanceMaintenanceInformation);

  void delete(InstanceMaintenanceInformation instanceMaintenanceInformation);

  void deleteById(long instanceId);

  InstanceMaintenanceInformation getById(long instanceId);

  void clear();

  List<InstanceMaintenanceInformation> listAll();
}
