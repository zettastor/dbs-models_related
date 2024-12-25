
package py.icshare.authorization;

import java.util.List;

public interface RoleStore {
  void saveRole(Role role);

  void deleteRole(Role role);

  Role getRoleById(long roleId);

  Role getRoleByName(String name);

  List<Role> listRoles();

  void cleanRoles();
}
