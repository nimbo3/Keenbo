package in.nimbo.dao.auth;

import in.nimbo.common.exception.DAOException;
import in.nimbo.entity.User;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class MySqlAuthDAO implements AuthDAO {
    private Connection connection;

    public MySqlAuthDAO(Connection connection) {
        this.connection = connection;
    }

    @Override
    public User authenticate(String username, String password) throws DAOException {
        try (PreparedStatement preparedStatement = connection.prepareStatement("SELECT * FROM users WHERE username=? AND password=?")){
            preparedStatement.setString(1, username);
            preparedStatement.setString(2, password);
            ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                String name = resultSet.getString("name");
                String email = resultSet.getString("email");
                String token = resultSet.getString("token");
                User user = new User(name, email, username, password, token);
                resultSet.close();
                return user;
            } else {
                resultSet.close();
                return null;
            }
        } catch (SQLException e) {
            throw new DAOException("Unable to establish database connection", e);
        }
    }

    @Override
    public boolean containsUserName(String username) {
        try (PreparedStatement preparedStatement = connection.prepareStatement("SELECT * FROM users WHERE username=?")){
            preparedStatement.setString(1, username);
            ResultSet resultSet = preparedStatement.executeQuery();
            boolean contains = resultSet.next();
            resultSet.close();
            return contains;
        } catch (SQLException e) {
            throw new DAOException("Unable to establish database connection", e);
        }
    }

    @Override
    public User register(String username, String password, String email, String name, String token) {
        try (PreparedStatement preparedStatement = connection.prepareStatement("INSERT INTO users (username, password, email, name, token) VALUES (?, ?, ?, ?, ?)")){
            preparedStatement.setString(1, username);
            preparedStatement.setString(2, password);
            preparedStatement.setString(3, email);
            preparedStatement.setString(4, name);
            preparedStatement.setString(5, token);
            preparedStatement.executeUpdate();
            return new User(name, email, username, password, token);
        } catch (SQLException e) {
            throw new DAOException("Unable to establish database connection", e);
        }
    }

    @Override
    public void saveClick(User user, String destination, int label) {
        try (PreparedStatement preparedStatement = connection.prepareStatement("INSERT INTO clicks (username, destination) VALUES (?, ?, ?)")){
            preparedStatement.setString(1, user.getUsername());
            preparedStatement.setString(2, destination);
            preparedStatement.setInt(3, label);
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            throw new DAOException("Unable to establish database connection", e);
        }
    }

    @Override
    public User authenticate(String token) {
        try (PreparedStatement preparedStatement = connection.prepareStatement("SELECT * FROM users WHERE token=?")){
            preparedStatement.setString(1, token);
            ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                String name = resultSet.getString("name");
                String email = resultSet.getString("email");
                String username = resultSet.getString("username");
                String password = resultSet.getString("password");
                User user = new User(name, email, username, password, token);
                resultSet.close();
                return user;
            } else {
                resultSet.close();
                return null;
            }
        } catch (SQLException e) {
            throw new DAOException("Unable to establish database connection", e);
        }
    }
}
