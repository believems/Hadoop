package com.github.believems.hadoop.helper;

import com.github.believems.hadoop.exception.PropertyException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Properties;

/**
 * Created by Administrator on 13-12-10.
 */
public final class PropertryHelper {
    /**
     * The logger.
     */
    private static final Logger LOG = LoggerFactory.getLogger(PropertryHelper.class);
    /**
     * Constant for the file URL protocol.
     */
    static final String PROTOCOL_FILE = "file";

    /**
     * Constant for the resource path separator.
     */
    static final String RESOURCE_PATH_SEPARATOR = "/";

    /**
     * Constant for the file URL protocol
     */
    private static final String FILE_SCHEME = "file:";

    /**
     * Constant for the name of the clone() method.
     */
    private static final String METHOD_CLONE = "clone";

    /**
     * Constant for parsing numbers in hex format.
     */
    private static final int HEX = 16;


    /**
     * Private constructor. Prevents instances from being created.
     */
    private PropertryHelper() {
        // to prevent instantiation...
    }

    public static InputStream getInputStream(String fileName){
        FileSystem fileSystem = FileSystem.getDefaultFileSystem();
        String basePath = null;
        Properties props = new Properties();
        URL url = locate(fileSystem, basePath, fileName);
        if (url == null) {
            throw new PropertyException("Cannot locate configuration source " + fileName);
        }
        InputStream in = null;
        try {
            in = fileSystem.getInputStream(url);
        } catch (PropertyException e) {
            throw e;
        }catch (Exception e) {
            throw new PropertyException("Unable to load the configuration from the URL " + url, e);
        }
        return in;
    }
    public static Properties loadProperties(String fileName)  {
        Properties props = new Properties();
        InputStream in = null;
        try {
            in = getInputStream(fileName);
            props.load(in);
        } catch (PropertyException e) {
            throw e;
        } catch (IOException e) {
            throw new PropertyException("Unable to load the configuration from the File " + fileName, e);
        } finally {
            // close the input stream
            try {
                if (in != null) {
                    in.close();
                }
            } catch (IOException e) {
                LOG.warn("Could not close input stream", e);
            }
        }
        return props;
    }

    /**
     * Constructs a URL from a base path and a file name. The file name can
     * be absolute, relative or a full URL. If necessary the base path URL is
     * applied.
     *
     * @param basePath the base path URL (can be <b>null</b>)
     * @param file     the file name
     * @return the resulting URL
     * @throws java.net.MalformedURLException if URLs are invalid
     */
    private static URL getURL(String basePath, String file) throws MalformedURLException {
        return FileSystem.getDefaultFileSystem().getURL(basePath, file);
    }

    /**
     * Helper method for constructing a file object from a base path and a
     * file name. This method is called if the base path passed to
     * {@code getURL()} does not seem to be a valid URL.
     *
     * @param basePath the base path
     * @param fileName the file name
     * @return the resulting file
     */
    private static File constructFile(String basePath, String fileName) {
        File file;

        File absolute = null;
        if (fileName != null) {
            absolute = new File(fileName);
        }

        if (basePath == null || basePath.isEmpty() || (absolute != null && absolute.isAbsolute())) {
            file = new File(fileName);
        } else {
            StringBuilder fName = new StringBuilder();
            fName.append(basePath);

            // My best friend. Paranoia.
            if (!basePath.endsWith(File.separator)) {
                fName.append(File.separator);
            }

            //
            // We have a relative path, and we have
            // two possible forms here. If we have the
            // "./" form then just strip that off first
            // before continuing.
            //
            if (fileName.startsWith("." + File.separator)) {
                fName.append(fileName.substring(2));
            } else {
                fName.append(fileName);
            }

            file = new File(fName.toString());
        }

        return file;
    }

    /**
     * Return the location of the specified resource by searching the user home
     * directory, the current classpath and the system classpath.
     *
     * @param name the name of the resource
     * @return the location of the resource
     */
    private static URL locate(String name) {
        return locate(null, name);
    }

    /**
     * Return the location of the specified resource by searching the user home
     * directory, the current classpath and the system classpath.
     *
     * @param base the base path of the resource
     * @param name the name of the resource
     * @return the location of the resource
     */
    private static URL locate(String base, String name) {
        return locate(FileSystem.getDefaultFileSystem(), base, name);
    }

    /**
     * Return the location of the specified resource by searching the user home
     * directory, the current classpath and the system classpath.
     *
     * @param fileSystem the FileSystem to use.
     * @param base       the base path of the resource
     * @param name       the name of the resource
     * @return the location of the resource
     */
    private static URL locate(FileSystem fileSystem, String base, String name) {
        if (LOG.isDebugEnabled()) {
            StringBuilder buf = new StringBuilder();
            buf.append("PropertryHelper.locate(): base is ").append(base);
            buf.append(", name is ").append(name);
            LOG.debug(buf.toString());
        }

        if (name == null) {
            // undefined, always return null
            return null;
        }

        // attempt to create an URL directly

        URL url = fileSystem.locateFromURL(base, name);

        // attempt to load from an absolute path
        if (url == null) {
            File file = new File(name);
            if (file.isAbsolute() && file.exists()) // already absolute?
            {
                try {
                    url = toURL(file);
                    LOG.debug("Loading configuration from the absolute path " + name);
                } catch (MalformedURLException e) {
                    LOG.warn("Could not obtain URL from file", e);
                }
            }
        }

        // attempt to load from the base directory
        if (url == null) {
            try {
                File file = constructFile(base, name);
                if (file != null && file.exists()) {
                    url = toURL(file);
                }

                if (url != null) {
                    LOG.debug("Loading configuration from the path " + file);
                }
            } catch (MalformedURLException e) {
                LOG.warn("Could not obtain URL from file", e);
            }
        }

        // attempt to load from the user home directory
        if (url == null) {
            try {
                File file = constructFile(System.getProperty("user.home"), name);
                if (file != null && file.exists()) {
                    url = toURL(file);
                }

                if (url != null) {
                    LOG.debug("Loading configuration from the home path " + file);
                }

            } catch (MalformedURLException e) {
                LOG.warn("Could not obtain URL from file", e);
            }
        }

        // attempt to load from classpath
        if (url == null) {
            url = locateFromClasspath(name);
        }
        return url;
    }

    /**
     * Tries to find a resource with the given name in the classpath.
     *
     * @param resourceName the name of the resource
     * @return the URL to the found resource or <b>null</b> if the resource
     * cannot be found
     */
    private static URL locateFromClasspath(String resourceName) {
        URL url = null;
        // attempt to load from the context classpath
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        if (loader != null) {
            url = loader.getResource(resourceName);

            if (url != null) {
                LOG.debug("Loading configuration from the context classpath (" + resourceName + ")");
            }
        }

        // attempt to load from the system classpath
        if (url == null) {
            url = ClassLoader.getSystemResource(resourceName);

            if (url != null) {
                LOG.debug("Loading configuration from the system classpath (" + resourceName + ")");
            }
        }
        return url;
    }

    /**
     * Return the path without the file name, for example http://xyz.net/foo/bar.xml
     * results in http://xyz.net/foo/
     *
     * @param url the URL from which to extract the path
     * @return the path component of the passed in URL
     */
    private static String getBasePath(URL url) {
        if (url == null) {
            return null;
        }

        String s = url.toString();

        if (s.startsWith(FILE_SCHEME) && !s.startsWith("file://")) {
            s = "file://" + s.substring(FILE_SCHEME.length());
        }
        String path = url.getPath();

        if (s.endsWith("/") || path == null || path.isEmpty()) {
            return s;
        } else {
            return s.substring(0, s.lastIndexOf("/") + 1);
        }
    }

    /**
     * Extract the file name from the specified URL.
     *
     * @param url the URL from which to extract the file name
     * @return the extracted file name
     */
    private static String getFileName(URL url) {
        if (url == null) {
            return null;
        }

        String path = url.getPath();

        if (path.endsWith("/") || path == null || path.isEmpty()) {
            return null;
        } else {
            return path.substring(path.lastIndexOf("/") + 1);
        }
    }

    /**
     * Tries to convert the specified base path and file name into a file object.
     * This method is called e.g. by the save() methods of file based
     * configurations. The parameter strings can be relative files, absolute
     * files and URLs as well. This implementation checks first whether the passed in
     * file name is absolute. If this is the case, it is returned. Otherwise
     * further checks are performed whether the base path and file name can be
     * combined to a valid URL or a valid file name. <em>Note:</em> The test
     * if the passed in file name is absolute is performed using
     * {@code java.io.File.isAbsolute()}. If the file name starts with a
     * slash, this method will return <b>true</b> on Unix, but <b>false</b> on
     * Windows. So to ensure correct behavior for relative file names on all
     * platforms you should never let relative paths start with a slash. E.g.
     * in a configuration definition file do not use something like that:
     * <pre>
     * &lt;properties fileName="/subdir/my.properties"/&gt;
     * </pre>
     * Under Windows this path would be resolved relative to the configuration
     * definition file. Under Unix this would be treated as an absolute path
     * name.
     *
     * @param basePath the base path
     * @param fileName the file name
     * @return the file object (<b>null</b> if no file can be obtained)
     */
    private static File getFile(String basePath, String fileName) {
        // Check if the file name is absolute
        File f = new File(fileName);
        if (f.isAbsolute()) {
            return f;
        }

        // Check if URLs are involved
        URL url;
        try {
            url = new URL(new URL(basePath), fileName);
        } catch (MalformedURLException mex1) {
            try {
                url = new URL(fileName);
            } catch (MalformedURLException mex2) {
                url = null;
            }
        }

        if (url != null) {
            return fileFromURL(url);
        }

        return constructFile(basePath, fileName);
    }

    /**
     * Tries to convert the specified URL to a file object. If this fails,
     * <b>null</b> is returned. Note: This code has been copied from the
     * {@code FileUtils} class from <em>Commons IO</em>.
     *
     * @param url the URL
     * @return the resulting file object
     */
    private static File fileFromURL(URL url) {
        if (url == null || !url.getProtocol().equals(PROTOCOL_FILE)) {
            return null;
        } else {
            String filename = url.getFile().replace('/', File.separatorChar);
            int pos = 0;
            while ((pos = filename.indexOf('%', pos)) >= 0) {
                if (pos + 2 < filename.length()) {
                    String hexStr = filename.substring(pos + 1, pos + 3);
                    char ch = (char) Integer.parseInt(hexStr, HEX);
                    filename = filename.substring(0, pos) + ch
                            + filename.substring(pos + 3);
                }
            }
            return new File(filename);
        }
    }

    /**
     * Convert the specified file into an URL. This method is equivalent
     * to file.toURI().toURL(). It was used to work around a bug in the JDK
     * preventing the transformation of a file into an URL if the file name
     * contains a '#' character. See the issue CONFIGURATION-300 for
     * more details. Now that we switched to JDK 1.4 we can directly use
     * file.toURI().toURL().
     *
     * @param file the file to be converted into an URL
     */
    private static URL toURL(File file) throws MalformedURLException {
        return file.toURI().toURL();
    }

    static abstract class FileSystem {
        /**
         * The name of the system property that can be used to set the file system class name
         */
        private static final String FILE_SYSTEM = "org.apache.commons.configuration.filesystem";

        /**
         * The default file system
         */
        private static FileSystem fileSystem;

        /**
         * The Logger
         */
        private Logger log;

        public FileSystem() {
            setLogger(null);
        }

        /**
         * Returns the logger used by this FileSystem.
         *
         * @return the logger
         */
        public Logger getLogger() {
            return log;
        }

        /**
         * Allows to set the logger to be used by this FileSystem. This
         * method makes it possible for clients to exactly control logging behavior.
         * Per default a logger is set that will ignore all log messages. Derived
         * classes that want to enable logging should call this method during their
         * initialization with the logger to be used.
         *
         * @param log the new logger
         */
        public void setLogger(Logger log) {
            this.log = (log != null) ? log : LoggerFactory.getLogger(FileSystem.class);
        }

        static {
            String fsClassName = System.getProperty(FILE_SYSTEM);
            if (fsClassName != null) {
                Logger log = LoggerFactory.getLogger(FileSystem.class);

                try {
                    Class<?> clazz = Class.forName(fsClassName);
                    if (FileSystem.class.isAssignableFrom(clazz)) {
                        fileSystem = (FileSystem) clazz.newInstance();
                        if (log.isDebugEnabled()) {
                            log.debug("Using " + fsClassName);
                        }
                    }
                } catch (InstantiationException ex) {
                    log.error("Unable to create " + fsClassName, ex);
                } catch (IllegalAccessException ex) {
                    log.error("Unable to create " + fsClassName, ex);
                } catch (ClassNotFoundException ex) {
                    log.error("Unable to create " + fsClassName, ex);
                }
            }

            if (fileSystem == null) {
                fileSystem = new DefaultFileSystem();
            }
        }

        /**
         * Set the FileSystem to use.
         *
         * @param fs The FileSystem
         * @throws NullPointerException if the FileSystem parameter is null.
         */
        public static void setDefaultFileSystem(FileSystem fs) throws NullPointerException {
            if (fs == null) {
                throw new NullPointerException("A FileSystem implementation is required");
            }
            fileSystem = fs;
        }

        /**
         * Reset the FileSystem to the default.
         */
        public static void resetDefaultFileSystem() {
            fileSystem = new DefaultFileSystem();
        }

        /**
         * Retrieve the FileSystem being used.
         *
         * @return The FileSystem.
         */
        public static FileSystem getDefaultFileSystem() {
            return fileSystem;
        }

        public abstract InputStream getInputStream(String basePath, String fileName)
                throws PropertyException;

        public abstract InputStream getInputStream(URL url) throws PropertyException;

        public abstract String getPath(File file, URL url, String basePath, String fileName);

        public abstract String getBasePath(String path);

        public abstract String getFileName(String path);

        public abstract URL locateFromURL(String basePath, String fileName);

        public abstract URL getURL(String basePath, String fileName) throws MalformedURLException;
    }

    static class DefaultFileSystem extends FileSystem {
        /**
         * The Log for diagnostic messages.
         */
        private Logger log = LoggerFactory.getLogger(DefaultFileSystem.class);

        @Override
        public InputStream getInputStream(String basePath, String fileName)
                throws PropertyException {
            try {
                URL url = PropertryHelper.locate(this, basePath, fileName);

                if (url == null) {
                    throw new PropertyException("Cannot locate configuration source " + fileName);
                }
                return getInputStream(url);
            } catch (PropertyException e) {
                throw e;
            } catch (Exception e) {
                throw new PropertyException("Unable to load the configuration file " + fileName, e);
            }
        }

        @Override
        public InputStream getInputStream(URL url) throws PropertyException {
            // throw an exception if the target URL is a directory
            File file = PropertryHelper.fileFromURL(url);
            if (file != null && file.isDirectory()) {
                throw new PropertyException("Cannot load a configuration from a directory");
            }

            try {
                return url.openStream();
            } catch (Exception e) {
                throw new PropertyException("Unable to load the configuration from the URL " + url, e);
            }
        }

        @Override
        public String getPath(File file, URL url, String basePath, String fileName) {
            String path = null;
            // if resource was loaded from jar file may be null
            if (file != null) {
                path = file.getAbsolutePath();
            }

            // try to see if file was loaded from a jar
            if (path == null) {
                if (url != null) {
                    path = url.getPath();
                } else {
                    try {
                        path = getURL(basePath, fileName).getPath();
                    } catch (Exception e) {
                        // simply ignore it and return null
                        if (log.isDebugEnabled()) {
                            log.debug(String.format("Could not determine URL for "
                                    + "basePath = %s, fileName = %s.", basePath,
                                    fileName), e);
                        }
                    }
                }
            }

            return path;
        }

        @Override
        public String getBasePath(String path) {
            URL url;
            try {
                url = getURL(null, path);
                return PropertryHelper.getBasePath(url);
            } catch (Exception e) {
                return null;
            }
        }

        @Override
        public String getFileName(String path) {
            URL url;
            try {
                url = getURL(null, path);
                return PropertryHelper.getFileName(url);
            } catch (Exception e) {
                return null;
            }
        }


        @Override
        public URL getURL(String basePath, String file) throws MalformedURLException {
            File f = new File(file);
            if (f.isAbsolute()) // already absolute?
            {
                return PropertryHelper.toURL(f);
            }

            try {
                if (basePath == null) {
                    return new URL(file);
                } else {
                    URL base = new URL(basePath);
                    return new URL(base, file);
                }
            } catch (MalformedURLException uex) {
                return PropertryHelper.toURL(PropertryHelper.constructFile(basePath, file));
            }
        }


        @Override
        public URL locateFromURL(String basePath, String fileName) {
            try {
                URL url;
                if (basePath == null) {
                    return new URL(fileName);
                    //url = new URL(name);
                } else {
                    URL baseURL = new URL(basePath);
                    url = new URL(baseURL, fileName);

                    // check if the file exists
                    InputStream in = null;
                    try {
                        in = url.openStream();
                    } finally {
                        if (in != null) {
                            in.close();
                        }
                    }
                    return url;
                }
            } catch (IOException e) {
                if (log.isDebugEnabled()) {
                    log.debug("Could not locate file " + fileName + " at " + basePath + ": " + e.getMessage());
                }
                return null;
            }
        }

        /**
         * Create the path to the specified file.
         *
         * @param file the target file
         */
        private void createPath(File file) {
            if (file != null) {
                // create the path to the file if the file doesn't exist
                if (!file.exists()) {
                    File parent = file.getParentFile();
                    if (parent != null && !parent.exists()) {
                        parent.mkdirs();
                    }
                }
            }
        }
    }
}