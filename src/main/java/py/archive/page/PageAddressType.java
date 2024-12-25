

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
