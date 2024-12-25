

package py.archive;

import com.fasterxml.jackson.annotation.JsonIgnore;

public abstract class Metadata {
  @JsonIgnore
  private ArchiveMetadata archiveMetadataBase;

  public Metadata(ArchiveMetadata archiveMetadataBase) {
    this.archiveMetadataBase = archiveMetadataBase;
  }

  public ArchiveMetadata getArchiveMetadataBase() {
    return archiveMetadataBase;
  }

  public void setArchiveMetadataBase(ArchiveMetadata archiveMetadataBase) {
    this.archiveMetadataBase = archiveMetadataBase;
  }
}
