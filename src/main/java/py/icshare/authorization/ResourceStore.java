
package py.icshare.authorization;

import java.util.List;

public interface ResourceStore {
  void saveResource(PyResource resource);

  void deleteResource(PyResource resource);

  void deleteResourceById(long resourceId);

  PyResource getResourceById(long resourceId);

  PyResource getResourceByName(String name);

  List<PyResource> listResources();

  void cleanResources();
}
