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

package py.volume;

import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The usage of this class is 1. VolumeMetadataJsonMerger merger = new
 * VolumeMetadataJsonMerger(...);
 *
 * <p>2. merger.add(...) 3. merger.add(...)
 *
 * <p>n. merger.merge()
 *
 * <p>This class only merge childVolumeId and getPositionOfFirstSegmentInLogicVolume of a volume
 *
 */
public class VolumeMetadataJsonMerger {
  private static final Logger logger = LoggerFactory.getLogger(VolumeMetadataJsonMerger.class);
  private VolumeMetadata myVolumeMetadata = null;
  private List<String> jsonStrings = new ArrayList<String>();

  public VolumeMetadataJsonMerger(String volumeMetadataJson) {
    if (volumeMetadataJson != null) {
      try {
        myVolumeMetadata = VolumeMetadata.fromJson(volumeMetadataJson);
      } catch (Exception e) {
        logger.error("Can't parse " + volumeMetadataJson, e);
      }
    }
  }

  public VolumeMetadata getMyVolumeMetadata() {
    return myVolumeMetadata;
  }

  public void add(String json) {
    if (json != null) {
      jsonStrings.add(json);
    }
  }

  /**
   * Merge volume meta.
   *
   * <p>currently, we only support 2 mutable field: positionOfFirstSegmentInLogicVolume and
   * childVolumeId. In future, we need to support mutable tag and tagValue. If so, we have to add a
   * version field in the volume metadata
   *
   * @return true if the my volume metadata is updated
   */
  public boolean merge() {
    boolean volumeUpdated = false;
    for (String json : jsonStrings) {
      try {
        VolumeMetadata volume = VolumeMetadata.fromJson(json);
        if (myVolumeMetadata == null) {
          myVolumeMetadata = volume;
          volumeUpdated = true;
          continue;
        }

        Long childId = volume.getChildVolumeId();
        int posOfFirstSegInLogicVolume = volume.getPositionOfFirstSegmentInLogicVolume();
        if (childId != null) {
          if (myVolumeMetadata.getChildVolumeId() == null) {
            myVolumeMetadata.setChildVolumeId(childId);
            volumeUpdated = true;
          } else if (childId != myVolumeMetadata.getChildVolumeId()) {
            String errString = "ChildId in json" + json
                + " is not equal to the one in my volume metadata" + myVolumeMetadata;
            logger.error(errString);
            throw new Exception(errString);
          }
        }

        if (posOfFirstSegInLogicVolume != 0) {
          if (myVolumeMetadata.getPositionOfFirstSegmentInLogicVolume() == 0) {
            myVolumeMetadata.setPositionOfFirstSegmentInLogicVolume(posOfFirstSegInLogicVolume);
            volumeUpdated = true;
          } else if (posOfFirstSegInLogicVolume != myVolumeMetadata
              .getPositionOfFirstSegmentInLogicVolume()) {
            String errString = "posOfFirstSegInLogicVolume in json" + json
                + " is not equal to the one in my volume metadata" + myVolumeMetadata;
            logger.error(errString);
            throw new Exception(errString);
          }
        }
      } catch (Exception e) {
        logger.warn("Failed to parse {} this might cause volume data lost", json, e);
      }
    }

    return volumeUpdated;
  }

}
