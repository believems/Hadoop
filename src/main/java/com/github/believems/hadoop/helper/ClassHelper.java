package com.github.believems.hadoop.helper;

import java.io.*;
import java.lang.reflect.Array;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.CodeSource;
import java.security.ProtectionDomain;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by Administrator on 14-3-19.
 */
public final class ClassHelper {
    private static ConcurrentMap<String, Class<?>> mappings = new ConcurrentHashMap<String, Class<?>>();
    static {
        addBaseClassMappings();
    }
    /**
     * 获取类所有的路径
     *
     * @param clazz
     * @return
     */
    public static String where(final Class clazz) {
        if (clazz == null) throw new IllegalArgumentException("null input: clazz");
        URL result = null;
        final String clsAsResource = clazz.getName().replace('.', '/').concat(".class");
        final ProtectionDomain pd = clazz.getProtectionDomain();
        if (pd != null) {
            final CodeSource cs = pd.getCodeSource();
            if (cs != null) result = cs.getLocation();
            if (result != null) {
                if ("file".equals(result.getProtocol())) {
                    try {
                        if (result.toExternalForm().endsWith(".jar") ||
                                result.toExternalForm().endsWith(".zip"))
                            result = new URL("jar:".concat(result.toExternalForm())
                                    .concat("!/").concat(clsAsResource));
                        else if (new File(result.getFile()).isDirectory())
                            result = new URL(result, clsAsResource);
                    } catch (MalformedURLException ignore) {
                    }
                }
            }
        }
        if (result == null) {
            final ClassLoader clsLoader = clazz.getClassLoader();
            result = clsLoader != null ?
                    clsLoader.getResource(clsAsResource) :
                    ClassLoader.getSystemResource(clsAsResource);
        }
        return result.toString();
    }

    /**
     * 当前执行该方法的类
     *
     * @return
     */
    public static Class currentClass() {
        StackTraceElement[] sts = Thread.currentThread().getStackTrace();
        if (SystemHelper.isAndroid()) {
            for (int i = 0; i < sts.length; i++) {
                if (sts[i].getClassName().equals(LogHelper.class.getName())) {
                    return sts[i + 1].getClass();
                }
            }
        }
        return sts[2].getClass();
    }

    /**
     * 对象克隆，支持单个对象，数组与集合
     *
     * @param src
     * @return
     */
    public static Object byteClone(Object src) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream out = new ObjectOutputStream(baos);
            out.writeObject(src);
            out.close();
            ByteArrayInputStream bin = new ByteArrayInputStream(
                    baos.toByteArray());
            ObjectInputStream in = new ObjectInputStream(bin);
            Object clone = in.readObject();
            in.close();
            return (clone);
        } catch (ClassNotFoundException e) {
            throw new InternalError(e.toString());
        } catch (StreamCorruptedException e) {
            throw new InternalError(e.toString());
        } catch (IOException e) {
            throw new InternalError(e.toString());
        }
    }

    /**
     * 是否是基本类型
     *
     * @param clazz
     * @return
     */
    public static Boolean isWrapClass(Class clazz) {
        try {
            return ((Class) clazz.getField("TYPE").get(null)).isPrimitive();
        } catch (Exception e) {
            return false;
        }
    }

    public static Class<?> getClass(Type type) {
        if (type.getClass() == Class.class) {
            return (Class<?>) type;
        }
        if (type instanceof ParameterizedType) {
            return getClass(((ParameterizedType) type).getRawType());
        }

        return Object.class;
    }

    public static boolean isGenericParamType(Type type) {
        if (type instanceof ParameterizedType) {
            return true;
        }
        if (type instanceof Class) {
            return isGenericParamType(((Class<?>) type).getGenericSuperclass());
        }

        return false;
    }

    public static Type getGenericParamType(Type type) {
        if (type instanceof ParameterizedType) {
            return type;
        }

        if (type instanceof Class) {
            return getGenericParamType(((Class<?>) type).getGenericSuperclass());
        }

        return type;
    }


    public static Class<?> loadClass(String className) {
        if (className == null || className.length() == 0) {
            return null;
        }

        Class<?> clazz = mappings.get(className);

        if (clazz != null) {
            return clazz;
        }

        if (className.charAt(0) == '[') {
            Class<?> componentType = loadClass(className.substring(1));
            return Array.newInstance(componentType, 0).getClass();
        }

        if (className.startsWith("L") && className.endsWith(";")) {
            String newClassName = className.substring(1, className.length() - 1);
            return loadClass(newClassName);
        }

        try {
            ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

            if (classLoader != null) {
                clazz = classLoader.loadClass(className);

                return clazz;
            }
        } catch (Throwable e) {
            // skip
        }

        try {
            clazz = Class.forName(className);

            addClassMapping(className, clazz);

            return clazz;
        } catch (Throwable e) {
            // skip
        }

        return clazz;
    }
    public static void addClassMapping(String className, Class<?> clazz) {
        if (className == null) {
            className = clazz.getName();
        }

        mappings.put(className, clazz);
    }
    public static void addBaseClassMappings() {
        mappings.put("byte", byte.class);
        mappings.put("short", short.class);
        mappings.put("int", int.class);
        mappings.put("long", long.class);
        mappings.put("float", float.class);
        mappings.put("double", double.class);
        mappings.put("boolean", boolean.class);
        mappings.put("char", char.class);

        mappings.put("[byte", byte[].class);
        mappings.put("[short", short[].class);
        mappings.put("[int", int[].class);
        mappings.put("[long", long[].class);
        mappings.put("[float", float[].class);
        mappings.put("[double", double[].class);
        mappings.put("[boolean", boolean[].class);
        mappings.put("[char", char[].class);

        mappings.put(HashMap.class.getName(), HashMap.class);
    }

    public static void clearClassMapping() {
        mappings.clear();
        addBaseClassMappings();
    }
}
