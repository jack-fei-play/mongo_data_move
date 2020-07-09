package com.finfosoft.test;

import org.junit.Test;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class FixedThreadPool {
    @Test
    public void testFixedThreadPool() {
        long l = System.currentTimeMillis();
        ExecutorService executorService = Executors.newFixedThreadPool(5);
        for (int i = 0; i < 1000000; i++) {
            final int index = i;
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    System.out.println(index+"线程：    "+Thread.currentThread().getName());
                }
            });
        }
        long l1 = System.currentTimeMillis();
        System.out.println(l1 - l);
    }

    @Test
    public void getInedx() throws Exception {
//        BufferedReader bufRead = new BufferedReader(new FileReader("E:\\work\\data_config_t.csv"));
//        ArrayList dataIdList = new ArrayList<Integer>();
//        String line;
//        while ((line = bufRead.readLine()) != null) {
//            int dataId = Integer.parseInt(line.replace("\"", ""));
//            dataIdList.add(dataId);
//        }
//        System.out.println(dataIdList);
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = format.parse("2019-5-13 10:9:24");
        System.out.println(date);
    }
}
