package com.cszt.util;

/**
 * Created by zjt on 2019/5/30.
 */
public class ClientConfig {
    private static String accessKey = "AV2I4CZ56E69GKHC2OGY";
    private static String secretKey = "AKpna9LLA2LxWJ9N0DgrOzMqBGlMNMF2sAwgXGzQ";
    private static String endPoint = "10.11.3.56:7480";
    private static int endBucketID = 100;   // 桶的数量(桶名为 bucket1, bucket2...)
    private static int ExpireDays = 180;  // 桶的过期天数

    public static String getAccessKey() {
        return accessKey;
    }

    public static String getSecretKey() {
        return secretKey;
    }

    public static String getEndPoint() {
        return endPoint;
    }

    public static int getEndBucketId() {
        return endBucketID;
    }

    public static int getExpireDays() {
        return ExpireDays;
    }
}
