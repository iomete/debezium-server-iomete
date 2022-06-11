package io.debezium.server.iomete.shared;

import java.security.SecureRandom;

import org.apache.commons.lang3.RandomStringUtils;

public class RandomData {
    static final SecureRandom rnd = new SecureRandom();

    public static int randomInt(int low, int high) {
        return rnd.nextInt(high - low) + low;
    }

    public static String randomString(int len) {
        return RandomStringUtils.randomAlphanumeric(len);
    }
}
