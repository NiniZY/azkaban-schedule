package com.concurrent;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ThreadPoolUtils {

    private static class ThreadPoolUtilsHoler{
        private static ThreadPoolExecutor threadPool=new ThreadPoolExecutor(10,
                20,
                10,
                TimeUnit.MINUTES,
                new ArrayBlockingQueue<Runnable>(10) );

    }

    public static ThreadPoolExecutor threadPool(){
        return ThreadPoolUtilsHoler.threadPool;
    }



}
