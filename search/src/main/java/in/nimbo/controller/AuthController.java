package in.nimbo.controller;

import in.nimbo.common.utility.LinkUtility;
import in.nimbo.config.SparkConfig;
import in.nimbo.dao.auth.AuthDAO;
import in.nimbo.dao.redis.LabelDAO;
import in.nimbo.entity.User;
import in.nimbo.response.ActionResult;

import java.util.Random;

public class AuthController {
    private AuthDAO authDAO;
    private SparkConfig config;
    private Random random;
    private LabelDAO labelDAO;

    public AuthController(AuthDAO authDAO, SparkConfig config, Random random, LabelDAO labelDAO) {
        this.authDAO = authDAO;
        this.config = config;
        this.random = random;
        this.labelDAO = labelDAO;
    }

    public ActionResult<User> login(String username, String password) {
        ActionResult<User> result = new ActionResult<>();
        User user = authDAO.authenticate(username.toLowerCase(), LinkUtility.hashLink(password));
        if (user != null) {
            result.setData(user);
            result.setSuccess(true);
        } else {
            result.setMessage(config.getLoginError());
        }
        return result;
    }

    public ActionResult<User> register(String username, String password, String confirmPass, String email,
                         String name) {
        ActionResult<User> result = new ActionResult<>();
        String errors = checkFields(username, password, confirmPass, email, name);
        if (errors.isEmpty()) {
            if (!authDAO.containsUserName(username.toLowerCase())) {
                result.setData(authDAO.register(username.toLowerCase(), LinkUtility.hashLink(password),
                        email, name, generateToken()));
                result.setSuccess(true);
            } else {
                result.setMessage(config.getUsernameDuplicateError());
            }
        } else {
            result.setMessage(errors);
        }
        return result;
    }

    private String generateToken() {
        StringBuilder tokenBuilder = new StringBuilder("");
        for (int i = 0; i < config.getTokenLength(); i++) {
            char ch = (char) (random.nextInt(26) + 'a');
            tokenBuilder.append(ch);
        }
        return tokenBuilder.toString();
    }

    public ActionResult<Boolean> click(User user, String destination) {
        ActionResult<Boolean> result = new ActionResult<>();
        if (user != null) {
            Integer label = labelDAO.get(destination);
            if (label != null) {
                authDAO.saveClick(user, destination, label);
                result.setSuccess(true);
            }
        }
        return result;
    }

    private String checkFields(String username, String password, String confirmPass, String email,
                             String name) {
        StringBuilder error = new StringBuilder("");
        if (username == null || username.isEmpty() || !username.matches("^[a-z]([a-z0-9]|_[a-z0-9]|.[a-z0-9])+$")) {
            error.append(config.getUsernameInvalidError());
            error.append("\n");
        }
        if (password == null || password.length() < config.getMinPasswordLength()) {
            error.append(config.getPasswordWeakError());
            error.append("\n");
        } else if (!password.equals(confirmPass)) {
            error.append(config.getPasswordUnlikeError());
            error.append("\n");
        }
        if (email == null || !email.matches("^[a-z]([a-z0-9]|_[a-z0-9]|.[a-z0-9])+@[a-z0-9_]+([.][a-z0-9]+)+$")) {
            error.append(config.getEmailInvalidError());
            error.append("\n");
        }
        if (name == null || name.isEmpty()) {
            error.append(config.getNameError());
        }
        return error.toString();
    }
}
