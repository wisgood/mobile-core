package com.bi.common.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MidUtil {
    private static MidUtil instance = new MidUtil();

    private static Map<Long, Long> aphoneMids = new HashMap<Long, Long>(10);

    private static List<Long> aphonePushTimeList = new ArrayList<Long>(10);

    private static Map<Long, Long> apadMids = new HashMap<Long, Long>(10);

    private static List<Long> apadPushTimeList = new ArrayList<Long>(10);

    private static Map<Long, Long> iphoneMids = new HashMap<Long, Long>(10);

    private static List<Long> iphonePushTimeList = new ArrayList<Long>(10);

    private static Map<Long, Long> ipadMids = new HashMap<Long, Long>(10);

    private static List<Long> ipadPushTimeList = new ArrayList<Long>(10);

    private static boolean init = false;

    private MidUtil() {
    }

    public static MidUtil getInstance(String midPath) throws IOException {

        init(midPath);
        return instance;

    }

    private static void init(String midPath) throws IOException {
        if (init)
            return;
        BufferedReader in = null;
        try {
            in = new BufferedReader(new InputStreamReader(new FileInputStream(
                    new File(midPath).getName())));
            String line = null;
            SimpleDateFormat dateFormat = new SimpleDateFormat(
                    "yyyy/MM/dd HH:mm");
            while ((line = in.readLine()) != null) {
                String[] fields = StringUtil.splitLog(line, ',');
                if (fields.length < 4)
                    continue;
                Long mid = Long.valueOf(fields[0]);
                Date pushTime = dateFormat.parse(fields[3]);
                if (fields[2].contains("aphone")) {
                    aphoneMids.put(mid, pushTime.getTime());
                    aphonePushTimeList.add(pushTime.getTime());
                }
                else if (fields[2].contains("apad")) {
                    apadMids.put(mid, pushTime.getTime());
                    apadPushTimeList.add(pushTime.getTime());
                }
                else if (fields[2].contains("iphone")) {
                    iphoneMids.put(mid, pushTime.getTime());
                    iphonePushTimeList.add(pushTime.getTime());
                }
                else if (fields[2].contains("ipad")) {
                    ipadMids.put(mid, pushTime.getTime());
                    ipadPushTimeList.add(pushTime.getTime());

                }
            }
            init = true;
        }
        catch(Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        finally {
            in.close();
        }

    }

    public boolean containMidFromAphone(String mid) {
        return aphoneMids.containsKey(Long.valueOf(mid));
    }

    public boolean containMidFromApad(String mid) {
        return apadMids.containsKey(Long.valueOf(mid));
    }

    public boolean containMidFromIphone(String mid) {
        return iphoneMids.containsKey(Long.valueOf(mid));
    }

    public boolean containMidFromIpad(String mid) {
        return ipadMids.containsKey(Long.valueOf(mid));
    }

    public boolean containMid(int plat, String mid) {
        switch (plat) {
        case 3:
            return containMidFromIpad(mid);
        case 4:
            return containMidFromIphone(mid);
        case 5:
            return containMidFromApad(mid);
        case 6:
            return containMidFromAphone(mid);
        default:
            return false;
        }

    }

    public boolean containMid(int plat, String midStr, long timestamp,
            long timespace) {
        switch (plat) {
        case 3:
            return containMid(ipadMids, midStr, timestamp, timespace);
        case 4:
            return containMid(iphoneMids, midStr, timestamp, timespace);
        case 5:
            return containMid(apadMids, midStr, timestamp, timespace);
        case 6:
            return containMid(aphoneMids, midStr, timestamp, timespace);
        default:
            return false;
        }

    }

    private boolean containMid(Map<Long, Long> midToPushTimeMap, String midStr,
            long timestamp, long timespace) {
        timestamp = timestamp * 1000;
        long mid = Long.valueOf(midStr);
        if (midToPushTimeMap.containsKey(mid)) {
            long pushTimestamp = midToPushTimeMap.get(mid);

            return (timestamp - pushTimestamp <= timespace)
                    && (timestamp - pushTimestamp > 0) ? true : false;
        }
        else {
            return false;
        }

    }

    public boolean containInTimeSpace(int plat, long timestamp, long timespace) {

        switch (plat) {
        case 3:
            return containInTimeSpace(ipadPushTimeList, timestamp, timespace);
        case 4:
            return containInTimeSpace(iphonePushTimeList, timestamp, timespace);
        case 5:
            return containInTimeSpace(apadPushTimeList, timestamp, timespace);
        case 6:
            return containInTimeSpace(aphonePushTimeList, timestamp, timespace);
        default:
            return false;
        }
    }

    private boolean containInTimeSpace(List<Long> pushList, long timestamp,
            long timespace) {
        timestamp = timestamp * 1000;
        for (int i = 0; i < pushList.size(); i++) {
            boolean isContainInTimeSpace = (timestamp - pushList.get(i) <= timespace)
                    && (timestamp - pushList.get(i) > 0);
            if (isContainInTimeSpace) {
                return isContainInTimeSpace;
            }

        }
        return false;
    }

    /**
     * @throws ParseException
     * 
     * @Title: main
     * @Description: 这里用一句话描述这个方法的作用
     * @param @param args 参数说明
     * @return void 返回类型说明
     * @throws
     */
    public static void main(String[] args) throws Exception {
        // TODO Auto-generated method stub
        MidUtil midUtil = MidUtil.getInstance("mid/20130930.txt");
        System.out.println(midUtil.containMidFromApad("183017"));
        System.out.println(midUtil.containMidFromApad("93069"));
        System.out.println(midUtil.containMidFromApad("930169"));
        System.out.println(midUtil.containMidFromAphone("183017"));
        System.out.println(midUtil.containMidFromAphone("93069"));
        System.out.println(midUtil.containMidFromAphone("930169"));
        System.out.println(midUtil.containMidFromIpad("183017"));
        System.out.println(midUtil.containMidFromIpad("93069"));
        System.out.println(midUtil.containMidFromIpad("930619"));
        System.out.println(midUtil.containMidFromIphone("183017"));
        System.out.println(midUtil.containMidFromIphone("93069"));
        System.out.println(midUtil.containMidFromIphone("930169"));

    }

}
