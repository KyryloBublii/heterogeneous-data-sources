package org.example.utils;

import java.util.UUID;

public class AppUtils {
    public static String generateUUID(){
        return UUID.randomUUID().toString().replaceAll("-", "");
    }
}
