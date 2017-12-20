package com.jnu.txtfile;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 正则表达式Utils
 */
public class DealStrSub {

    /**
     * 使用正则表达式匹配两个指定字符串中间的内容
     * @param soap
     * @param regx
     * @return
     */
    public static List<String> getSubUtil_1(String soap, String regx){
        List<String> list = new ArrayList<>();
        Pattern pattern = Pattern.compile(regx);//匹配模式
        Matcher matcher = pattern.matcher(soap);
        while(matcher.find()){
            int i = 1;
            list.add(matcher.group(i));
            i++;
        }
        return list;
    }

    public static String getSubUtil_2(String soap, String regx){
        Pattern pattern = Pattern.compile(regx);
        Matcher matcher = pattern.matcher(soap);
        while(matcher.find()){
            return matcher.group(1);
        }
        return "";

    }

    /**
     * 获取字符串中最后的数字
     * @param soap
     * @return
     */
    public static String getLastNum(String soap){
        Pattern pattern = Pattern.compile("\\d+$");
        Matcher matcher = pattern.matcher(soap);
        //System.out.println(matcher.find());
        while(matcher.find()){
            return matcher.group();
        }
        return "";
    }
}
