package com.utils;

import com.alibaba.fastjson.JSONArray;

public class StringUtils {
    public static String arrayToJSONString(String[] array){
        return JSONArray.toJSONString(array);
    }

    public static void main(String[] args) {
        String[] a={"aaa","bbb"};
        System.out.println(StringUtils.arrayToJSONString(a));
    }
}
