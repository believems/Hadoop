package com.github.believems.hadoop.helper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by Administrator on 13-12-16.
 */
public final class LogHelper {
    private static ConcurrentMap<String, Logger> Loggers = new ConcurrentHashMap<String, Logger>();

    public static Logger getLogger(Class clazz) {
        return getLogger(clazz.getName());
    }

    public static Logger getLogger(String clazzName) {
        Logger logger = Loggers.get(clazzName);
        if (logger == null) {
            logger = LoggerFactory.getLogger(clazzName);
            Loggers.put(clazzName, logger);
        }
        return logger;
    }

    public static Logger getLogger() {
        return getLogger(ClassHelper.currentClass());
    }

    public static void debug(String msg) {
        getLogger().debug(msg);
    }

    public static void info(String msg) {
        getLogger().info(msg);
    }

    public static void warn(String msg) {
        getLogger().warn(msg);
    }

    public static void error(Throwable e) {
        getLogger().error(e.getMessage(), e);
    }

    public static void error(String title, Throwable e) {
        getLogger().error(title, e);
    }
}
