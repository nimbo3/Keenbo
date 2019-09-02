package in.nimbo.controller;

import in.nimbo.common.utility.LinkUtility;
import in.nimbo.config.SparkConfig;
import in.nimbo.dao.auth.AuthDAO;
import in.nimbo.dao.redis.LabelDAO;
import in.nimbo.entity.User;
import in.nimbo.response.ActionResult;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Random;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class AuthControllerTest {
    private static AuthController controller;
    private static AuthDAO dao;
    private static SparkConfig config;
    private static LabelDAO labelDAO;

    @BeforeClass
    public static void init() {
        config = SparkConfig.load();
        dao = mock(AuthDAO.class);
        Random random = new Random();
        labelDAO = mock(LabelDAO.class);
        controller = new AuthController(dao, config, random, labelDAO);
    }

    @Test
    public void testTrueRegister() {
        String username = "admin";
        String password = "password";
        String confirmPass = "password";
        String email = "email@gmail.com";
        String token = "token";
        String name = "name";
        User user = new User(name, email, username, email, token);
        when(dao.register(anyString(), anyString(), anyString(), anyString(), anyString())).thenReturn(user);
        when(dao.containsUserName(username)).thenReturn(false);
        ActionResult<User> register = controller.register(username, password, confirmPass, email, name);
        assertTrue(register.isSuccess());
        assertEquals(register.getData(), user);
    }

    @Test
    public void testFalseRegister() {
        String username = " admin ";
        String password = "aa";
        String confirmPass = "a";
        String email = "admin";
        String name = "";
        ActionResult<User> register = controller.register(username, password, confirmPass, email, name);
        assertTrue(register.getMessage().contains(config.getEmailInvalidError()));
        assertTrue(register.getMessage().contains(config.getNameError()));
        assertTrue(register.getMessage().contains(config.getPasswordWeakError()));
        assertTrue(register.getMessage().contains(config.getEmailInvalidError()));
        assertTrue(register.getMessage().contains(config.getNameError()));
        assertFalse(register.isSuccess());
        assertNull(register.getData());
        password = "aaaaaa";
        register = controller.register(username, password, confirmPass, email, name);
        assertTrue(register.getMessage().contains(config.getPasswordUnlikeError()));
        assertFalse(register.isSuccess());
        assertNull(register.getData());
        register = controller.register(null, null, null, null, null);
        assertTrue(register.getMessage().contains(config.getEmailInvalidError()));
        assertTrue(register.getMessage().contains(config.getNameError()));
        assertTrue(register.getMessage().contains(config.getPasswordWeakError()));
        assertTrue(register.getMessage().contains(config.getEmailInvalidError()));
        assertTrue(register.getMessage().contains(config.getNameError()));
        assertFalse(register.isSuccess());
        assertNull(register.getData());
    }

    @Test
    public void testDuplicateRegister() {
        String username = "admin";
        String password = "password";
        String confirmPass = "password";
        String email = "email@gmail.com";
        String name = "name";
        when(dao.containsUserName(username)).thenReturn(true);
        ActionResult<User> register = controller.register(username, password, confirmPass, email, name);
        assertFalse(register.isSuccess());
        assertNull(register.getData());
    }

    @Test
    public void testLogin() {
        String username = "admin";
        String password = "password";
        User user = new User("name", "email", username, password, "token");
        when(dao.authenticate(username, LinkUtility.hashLink(password))).thenReturn(user);
        ActionResult<User> login = controller.login(username, password);
        assertTrue(login.isSuccess());
        assertEquals(login.getData(), user);
        when(dao.authenticate(username, LinkUtility.hashLink(password))).thenReturn(null);
        login = controller.login(username, password);
        assertFalse(login.isSuccess());
        assertNull(login.getData());
        assertTrue(login.getMessage().equals(config.getLoginError()));
    }

    @Test
    public void testClick() {
        User user = new User();
        String destination = "https://stackoverflow.com";
        when(labelDAO.get(destination)).thenReturn(1);
        doNothing().when(dao).saveClick(user, destination, 1);
        ActionResult<Boolean> click = controller.click(user, destination);
        assertTrue(click.isSuccess());
        click = controller.click(null, destination);
        assertFalse(click.isSuccess());

    }
}
