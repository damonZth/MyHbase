package com.jnu.pyramidUtils;

import java.util.ArrayList;
import java.util.List;

public class Test {
    public static void main(String[] args){
        double a = 1.9;
        double b = 1.4;
        System.out.println(Math.abs(b-a));


        List<Double> list = new ArrayList<>();
        list.add(0.99);
        list.add(1.22);

        System.out.println(Math.abs(list.get(1) - list.get(0)));
    }
}
