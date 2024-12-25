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

package py.archive.segment;

/**
 * Segment Unit status conflict type. Data Node will report their segment unit to information
 * center. But Segment unit 's status or something else may conflict with volume status. Information
 * center will throws exception with the cause
 */
public enum SegmentUnitStatusConflictCause {
  VolumeDeleted,                       // volume has been deleted
  VolumeRecycled,                      // volume is recycled after deleted
  StaleSnapshotVersion,                // snapshot version is lower than information center's
  RollbackToSnapshot,                  // volume is rolling back to snapshot
  VolumeExtendFailed,                  // volume extend failed
}
