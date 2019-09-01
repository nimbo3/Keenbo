package in.nimbo.entity;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

public class User {
    @JsonProperty("name")
    private String name;
    @JsonIgnore
    private String email;
    @JsonIgnore
    private String username;
    @JsonIgnore
    private String password;
    @JsonProperty("token")
    private String token;

    public User() {
    }

    public User(String name, String email, String username, String password, String token) {
        this.name = name;
        this.email = email;
        this.username = username;
        this.password = password;
        this.token = token;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }
}
