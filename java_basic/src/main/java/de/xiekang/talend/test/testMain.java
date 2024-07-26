package de.xiekang.talend.test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

public class testMain {
    public static void main(String... args) {
        System.out.println(String.format("%s%s", "123", "/" + 456));
        System.out.println(String.format("%s%s", 123, ""));
        String test = "LIMA_123123";
        System.out.println(test.substring(5));
    }
}
