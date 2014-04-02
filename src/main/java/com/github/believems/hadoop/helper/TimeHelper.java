package com.github.believems.hadoop.helper;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created with IntelliJ IDEA.
 * User: Administrator
 * Date: 13-11-19
 * Time: 下午5:20
 * To change this template use File | Settings | File Templates.
 */
public final class TimeHelper {
    private static AtomicBoolean simulating = new AtomicBoolean(false);
    private static volatile Map<Thread, AtomicLong> threadSleepTimes;
    private static final Object sleepTimesLock = new Object();

    private static AtomicLong simulatedCurrTimeMs; //should this be a thread local that's allowed to keep advancing?

    public static void startSimulating() {
        simulating.set(true);
        simulatedCurrTimeMs = new AtomicLong(0);
        threadSleepTimes = new ConcurrentHashMap<Thread, AtomicLong>();
    }

    public static void stopSimulating() {
        simulating.set(false);
        threadSleepTimes = null;
    }
    private static final SimpleDateFormat zdateFormatter = new SimpleDateFormat("yyyyMMdd");
    private static final SimpleDateFormat ztimeFormatter = new SimpleDateFormat("HHmmss");
    private static final SimpleDateFormat timestampFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    public static Integer[] toDateTime(Long timeStamp) {
        Calendar cale = Calendar.getInstance(Locale.ENGLISH);
        cale.setTimeInMillis(timeStamp- java.util.TimeZone.getDefault().getRawOffset());
        Integer zdate = Integer.parseInt(zdateFormatter.format(cale.getTime()));
        Integer ztime = Integer.parseInt(ztimeFormatter.format(cale.getTime()));
        return new Integer[]{zdate, ztime};
    }

    public static Timestamp toTimestamp(Long timestamp){
        Calendar cale = Calendar.getInstance(Locale.ENGLISH);
        cale.setTimeInMillis(timestamp- java.util.TimeZone.getDefault().getRawOffset());
        return new Timestamp(cale.getTimeInMillis());
    }

    public static String toFormatDate(Long timestamp){
        Calendar cale = Calendar.getInstance(Locale.ENGLISH);
        cale.setTimeInMillis(timestamp- java.util.TimeZone.getDefault().getRawOffset());
        return zdateFormatter.format(cale.getTime());
    }
    public static boolean isSimulating() {
        return simulating.get();
    }

    public static void sleepUntil(long targetTimeMs) throws InterruptedException {
        if (simulating.get()) {
            try {
                synchronized (sleepTimesLock) {
                    threadSleepTimes.put(Thread.currentThread(), new AtomicLong(targetTimeMs));
                }
                while (simulatedCurrTimeMs.get() < targetTimeMs) {
                    Thread.sleep(10);
                }
            } finally {
                synchronized (sleepTimesLock) {
                    threadSleepTimes.remove(Thread.currentThread());
                }
            }
        } else {
            long sleepTime = targetTimeMs - currentTimeMillis();
            if (sleepTime > 0)
                Thread.sleep(sleepTime);
        }
    }

    public static void sleep(long ms) throws InterruptedException {
        sleepUntil(currentTimeMillis() + ms);
    }

    public static long currentTimeMillis() {
        if (simulating.get()) {
            return simulatedCurrTimeMs.get();
        } else {
            Calendar cal = Calendar.getInstance(Locale.ENGLISH);
            cal.setTimeInMillis(cal.getTimeInMillis() - java.util.TimeZone.getDefault().getRawOffset());
            return cal.getTimeInMillis();
        }
    }

    public static int currentTimeSecs() {
        return (int) (currentTimeMillis() / 1000);
    }

    public static void advanceTime(long ms) {
        if (!simulating.get()) throw new IllegalStateException("Cannot simulate time unless in simulation mode");
        simulatedCurrTimeMs.set(simulatedCurrTimeMs.get() + ms);
    }

    public static boolean isThreadWaiting(Thread t) {
        if (!simulating.get()) throw new IllegalStateException("Must be in simulation mode");
        AtomicLong time;
        synchronized (sleepTimesLock) {
            time = threadSleepTimes.get(t);
        }
        return !t.isAlive() || time != null && currentTimeMillis() < time.longValue();
    }
}
