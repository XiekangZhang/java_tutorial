package de.xiekang.talend.test;

import java.util.List;

public class Test {
    private static List<String> stringList;

    public static void setStringList(List<String> strings) {
        stringList = strings;
    }

    public static List<String> getStringList() {
        return stringList;
    }
}
