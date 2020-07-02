package com.finfosoft.test;

import org.junit.Test;

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
}
