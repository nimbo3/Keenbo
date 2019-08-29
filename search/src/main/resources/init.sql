DROP DATABASE IF EXISTS keenbo;
CREATE DATABASE keenbo;
USE keenbo;

CREATE TABLE users (
  username VARCHAR(100) PRIMARY KEY,
  password TEXT NOT NULL ,
  name TEXT NOT NULL ,
  email TEXT NOT NULL ,
  token TEXT NOT NULL
);

CREATE TABLE clicks (
  id INT(11) PRIMARY KEY AUTO_INCREMENT,
  username VARCHAR(100) NOT NULL,
  destination VARCHAR(2048) NOT NULL,
  FOREIGN KEY (username) REFERENCES users(username)
);

CREATE TABLE searches (
  id INT(11) PRIMARY KEY AUTO_INCREMENT,
  username VARCHAR(100) NOT NULL,
  text TEXT NOT NULL,
  FOREIGN KEY (username) REFERENCES users(username)
);
