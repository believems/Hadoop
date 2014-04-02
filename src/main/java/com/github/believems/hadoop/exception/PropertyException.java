package com.github.believems.hadoop.exception;

/**
 * Created by IntelliJ IDEA.
 * User: 相利
 * Date: 11-12-30
 * Time: 下午3:51
 * To change this template use File | Settings | File Templates.
 */
public class PropertyException extends RuntimeException {

    public PropertyException(String message) {
        super(message);
    }

    public PropertyException(String message, Throwable cause) {
        super(message, cause);
    }

    public PropertyException() {
    }

    public PropertyException(Throwable cause) {
        super(cause);
    }
}
