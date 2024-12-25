
package py.icshare.authorization;

import java.util.List;

public interface ApiStore {
  List<ApiToAuthorize> listApis();

  void saveApi(ApiToAuthorize api);

  void deleteApi(ApiToAuthorize api);

  void cleanApis();
}
