package com.github.believems.hadoop;

import com.github.believems.hadoop.helper.PropertryHelper;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

/**
 * Created by Administrator on 2014/4/2.
 */
public class VERSION {
    private static final Properties hadoopProperties = PropertryHelper.loadProperties("build.properties");
    private static final DateFormat dateFormatter = new SimpleDateFormat("yyyyMMdd.HHmmss");
    public final static int MajorVersion;
    public final static int MinorVersion;
    public final static int RevisionVersion;

    static {
        String versionMajor = hadoopProperties.getProperty("version.major");
        String versionMinor = hadoopProperties.getProperty("version.minor");
        String versionRevision = hadoopProperties.getProperty("version.revision");

        MajorVersion = Integer.parseInt(versionMajor);
        MinorVersion = Integer.parseInt(versionMinor);
        RevisionVersion = Integer.parseInt(versionRevision);
    }

    public final static int VersionCode = MajorVersion * 10000 + MinorVersion * 100 + RevisionVersion;

    public final static Date BuildDate;

    static {
        Date BuildDateDefault;
        try {
            BuildDateDefault = dateFormatter.parse(hadoopProperties.getProperty("buildTimestamp"));
        } catch (ParseException e) {
            BuildDateDefault = new Date();
        }
        BuildDate = BuildDateDefault;
    }
}
