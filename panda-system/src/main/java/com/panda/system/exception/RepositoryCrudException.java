package com.panda.system.exception;

public class RepositoryCrudException extends RuntimeException {
  public RepositoryCrudException(String message, Throwable e) {
    super(message, e);
  }
}
