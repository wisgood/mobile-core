package com.bi.comprehensive.uinfy.user;

import java.util.ArrayList;
import java.util.List;

public class StringSplit {
    public static String[] splitLog(String content, char separator) {
        int length = content.length();
        int beginIndex = 0;
        List<String> pathStrings = new ArrayList<String>();
        for (int endIndex = 0; endIndex < length; endIndex++) {
            char ch = content.charAt(endIndex);
            if (ch == separator) {
                pathStrings.add(content.substring(beginIndex, endIndex));
                beginIndex = endIndex + 1;
            }
        }
        pathStrings.add(content.substring(beginIndex, length));

        return pathStrings.toArray(new String[0]);
    }
}