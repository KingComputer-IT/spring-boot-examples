package com.neo.test;

import org.junit.Test;

public class Test1 {

    @Test
    public void test1() {
        int x = 0;
        for (int i = 0; i <6 ; i++) {
            for (int j = i; j <i ; j++) {
                x++;
            }
        }
        System.out.println("x="+x);
    }
}
