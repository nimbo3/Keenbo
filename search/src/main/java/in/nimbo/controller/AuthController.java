package in.nimbo.controller;

import in.nimbo.config.SparkConfig;
import in.nimbo.dao.auth.AuthDAO;
import in.nimbo.entity.User;
import in.nimbo.response.ActionResult;

public class AuthController {
    private AuthDAO authDAO;
    private SparkConfig config;

    public AuthController(AuthDAO authDAO, SparkConfig config) {
        this.authDAO = authDAO;
        this.config = config;
    }

    public ActionResult<User> login(String username, String password) {
        ActionResult<User> result = new ActionResult<>();
        User user = authDAO.authenticate(username, password);
        if (user != null) {
            result.setData(user);
            result.setSuccess(true);
        } else {
            result.setMessage("نام کاربری یا پسورد اشتباه است");
        }
        return result;
    }

    public ActionResult<User> register(String username, String password, String confirmPass, String email,
                         String name) {
        ActionResult<User> result = new ActionResult<>();
        String errors = checkFields(username, password, confirmPass, email, name);
        if (errors.isEmpty()) {
            if (!authDAO.containsUserName(username)) {
                result.setData(authDAO.register(username, password, email, name));
                result.setSuccess(true);
            } else {
                result.setMessage("نام کاربری قبلا ثبت شده است");
            }
        } else {
            result.setMessage(errors);
        }
        return result;
    }

    public void click(User user, String destination) {

    }

    private String checkFields(String username, String password, String confirmPass, String email,
                             String name) {
        StringBuilder error = new StringBuilder("");
        if (username == null || username.isEmpty() || !username.matches("^[a-z]([a-z0-9]|_[a-z0-9]|.[a-z0-9])+$")) {
            error.append("نام کاربری معتبر نیست");
            error.append("\n");
        }
        if (password == null || password.length() < config.getMinPasswordLength()) {
            error.append("کلمه عبور ضعیف است");
            error.append("\n");
        } else if (!password.equals(confirmPass)) {
            error.append("کلمه عبور و تکرار آن باید یکسان باشد");
            error.append("\n");
        }
        if (email == null || !email.matches("^[a-z]([a-z0-9]|_[a-z0-9]|.[a-z0-9])+@[a-z0-9_]+([.][a-z0-9]+)+$")) {
            error.append("ایمیل معتبر نیست");
            error.append("\n");
        }
        if (name == null || name.isEmpty()) {
            error.append("نام خود را وارد نمایید");
        }
        return error.toString();
    }
}
