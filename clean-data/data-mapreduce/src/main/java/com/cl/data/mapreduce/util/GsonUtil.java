package com.cl.data.mapreduce.util;

import com.google.gson.Gson;

public class GsonUtil {
    private static Gson gson;

    static {
        gson = new Gson();
    }

    public static Gson getGson() {
        return gson;
    }
}
