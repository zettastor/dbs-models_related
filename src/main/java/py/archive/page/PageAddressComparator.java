
package py.archive.page;

import java.util.Comparator;

public class PageAddressComparator implements Comparator<PageAddress> {
  @Override
  public int compare(PageAddress o1, PageAddress o2) {
    if (o1 == null) {
      if (o2 == null) {
        return 0;
      } else {
        return -1;
      }
    }

    return o1.compareTo(o2);
  }
}
