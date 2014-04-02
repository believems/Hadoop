package com.github.believems.hadoop.helper;

import java.io.IOException;
import java.net.URL;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: Administrator
 * Date: 13-11-19
 * Time: 下午5:18
 * To change this template use File | Settings | File Templates.
 */
public final class SystemHelper {

    public static void sleep(long millis) {
        try {
            TimeHelper.sleep(millis);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static List<URL> findResources(String name) {
        try {
            Enumeration<URL> resources = Thread.currentThread().getContextClassLoader().getResources(name);
            List<URL> ret = new ArrayList<URL>();
            while (resources.hasMoreElements()) {
                ret.add(resources.nextElement());
            }
            return ret;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 当前运行的 Java 虚拟机是否是在安卓环境
     */
    public static Boolean isAndroid() {
        try {
            Class.forName("android.Manifest");
            return true;
        } catch (Throwable e) {
            return false;
        }
    }
    /**
     * 使用当前线程的ClassLoader加载给定的类
     *
     * @param className
     *            类的全称
     * @return 给定的类
     * @throws ClassNotFoundException
     *             如果无法用当前线程的ClassLoader加载
     */
    public static Class<?> loadClass(String className)
            throws ClassNotFoundException {
        try {
            return Thread.currentThread()
                    .getContextClassLoader()
                    .loadClass(className);
        }
        catch (ClassNotFoundException e) {
            return Class.forName(className);
        }
    }
    public static Map readCommandLineOpts() {
        Map ret = new HashMap();
        String commandOptions = System.getProperty("storm.options");
        if (commandOptions != null) {
            commandOptions = commandOptions.replaceAll("%%%%", " ");
            String[] configs = commandOptions.split(",");
            for (String config : configs) {
                String[] options = config.split("=");
                if (options.length == 2) {
                    ret.put(options[0], options[1]);
                }
            }
        }
        return ret;
    }

    private static Object normalizeConf(Object conf) {
        if (conf == null) return new HashMap();
        if (conf instanceof Map) {
            Map confMap = new HashMap((Map) conf);
            for (Object key : confMap.keySet()) {
                Object val = confMap.get(key);
                confMap.put(key, normalizeConf(val));
            }
            return confMap;
        } else if (conf instanceof List) {
            List confList = new ArrayList((List) conf);
            for (int i = 0; i < confList.size(); i++) {
                Object val = confList.get(i);
                confList.set(i, normalizeConf(val));
            }
            return confList;
        } else if (conf instanceof Integer) {
            return ((Integer) conf).longValue();
        } else if (conf instanceof Float) {
            return ((Float) conf).doubleValue();
        } else {
            return conf;
        }
    }

    public static <S, T> T get(Map<S, T> m, S key, T def) {
        T ret = m.get(key);
        if (ret == null) {
            ret = def;
        }
        return ret;
    }

    public static List<Object> tuple(Object... values) {
        List<Object> ret = new ArrayList<Object>();
        for (Object v : values) {
            ret.add(v);
        }
        return ret;
    }

    public static boolean isSystemId(String id) {
        return id.startsWith("__");
    }

    public static <K, V> Map<V, K> reverseMap(Map<K, V> map) {
        Map<V, K> ret = new HashMap<V, K>();
        for (K key : map.keySet()) {
            ret.put(map.get(key), key);
        }
        return ret;
    }

    public static Integer getInt(Object o) {
        if (o instanceof Long) {
            return ((Long) o).intValue();
        } else if (o instanceof Integer) {
            return (Integer) o;
        } else if (o instanceof Short) {
            return ((Short) o).intValue();
        } else {
            throw new IllegalArgumentException("Don't know how to convert " + o + " + to int");
        }
    }

    public static long secureRandomLong() {
        return UUID.randomUUID().getLeastSignificantBits();
    }


    /**
     * (defn integer-divided [sum num-pieces]
     * (let [base (int (/ sum num-pieces))
     * num-inc (mod sum num-pieces)
     * num-bases (- num-pieces num-inc)]
     * (if (= num-inc 0)
     * {base num-bases}
     * {base num-bases (inc base) num-inc}
     * )))
     *
     * @param sum
     * @param numPieces
     * @return
     */

    public static TreeMap<Integer, Integer> integerDivided(int sum, int numPieces) {
        int base = sum / numPieces;
        int numInc = sum % numPieces;
        int numBases = numPieces - numInc;
        TreeMap<Integer, Integer> ret = new TreeMap<Integer, Integer>();
        ret.put(base, numBases);
        if (numInc != 0) {
            ret.put(base + 1, numInc);
        }
        return ret;
    }
    public static boolean exceptionCauseIsInstanceOf(Class clazz, Throwable throwable) {
        Throwable t = throwable;
        while (t != null) {
            if (clazz.isInstance(t)) {
                return true;
            }
            t = t.getCause();
        }
        return false;
    }
}