package com.scl;

import org.junit.Test;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Test3 {

    @Test
    public void test(){
        Pattern pattern = Pattern.compile(".");
        Matcher matcher = pattern.matcher("12839430324");

        while (matcher.find()){
            System.out.println(matcher.toString());
            System.out.println(matcher.group());
        }
    }

    @Test
    public void test_1(){
        System.out.println(0|1);
    }
}
