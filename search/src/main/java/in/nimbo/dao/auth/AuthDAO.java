package in.nimbo.dao.auth;

import in.nimbo.entity.User;

public interface AuthDAO {

    User authenticate(String username, String password);

    boolean containsUserName(String username);

    User register(String username, String password, String email, String name, String token);

    void saveClick(User user, String destination);
}
