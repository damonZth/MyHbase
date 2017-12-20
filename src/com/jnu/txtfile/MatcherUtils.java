package com.jnu.txtfile;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MatcherUtils {

    /**
     * 根据id提取文本中的id值
     * @param str
     */
    public static String IdMatch(String str){
        String reStr = "";
        Pattern pattern = Pattern.compile("[0-9]+");
        Matcher matcher = pattern.matcher(str);
        while(matcher.find()){
            reStr = matcher.group();
        }
        return reStr;
    }

    /**
     * 匹配出 ASIN 的值
     * @param str
     */
    public static String ASINMatch(String str){
        String reStr = "";
        Pattern pattern = Pattern.compile("[0-9]{10}");
        Matcher matcher = pattern.matcher(str);
        while(matcher.find()){
            //System.out.println("ASIN: " + matcher.group());
            reStr = matcher.group();
        }
        return reStr;
    }

    /**
     * 提取标题
     * @param str
     * @return
     */
    public static String TitleMatch(String str){
//        String reStr = "";
//        Pattern pattern = Pattern.compile("[\\s\\S]+");
//        Matcher matcher = pattern.matcher(str);
//        while(matcher.find()){
//            //System.out.println("ASIN: " + matcher.group());
//            reStr = matcher.group();
//        }
//        return reStr;
        return str.substring(9);
    }

    /**
     * 提取 Group
     * @param str
     * @return
     */
    public static String GroupMatch(String str){
//        String reStr = "";
//        Pattern pattern = Pattern.compile("(^[group:])[\\s\\S]+");
//        Matcher matcher = pattern.matcher(str);
//        while(matcher.find()){
//            //System.out.println("ASIN: " + matcher.group());
//            reStr = matcher.group();
//        }
//        return reStr;
        return str.substring(9);
    }

    /**
     * 提取 SalesRank
     * @param str
     * @return
     */
    public static String SalesrankMatch(String str){
        String reStr = "";
        Pattern pattern = Pattern.compile("[0-9]+");
        Matcher matcher = pattern.matcher(str);
        while(matcher.find()){
            //System.out.println("ASIN: " + matcher.group());
            reStr = matcher.group();
        }
        return reStr;

        //return str.substring(13);
    }

    /**
     * 提取出用户的评论时间
     */
    public static String TimeMatch(String str){
        String reStr = "";
        Pattern pattern = Pattern.compile("[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}");
        Matcher matcher = pattern.matcher(str);
        while(matcher.find()){
            //System.out.println("ASIN: " + matcher.group());
            reStr = matcher.group();
        }
        return reStr;
    }

    public static String CutomerMatch(String str){
        String regex = "cutomer:\\s+(.*?)\\s+rating";
        return DealStrSub.getSubUtil_2(str,regex);
    }

    public static String RatingMatch(String str){
        String regex = "rating:\\s+(.*?)\\s+votes";
        return DealStrSub.getSubUtil_2(str,regex);
    }

    public static String VoteMatch(String str){
        String regex = "votes:\\s+(.*?)\\s+";
        return DealStrSub.getSubUtil_2(str, regex);
    }

    public static String HelpfulMatch(String str){
        return DealStrSub.getLastNum(str);
    }

    private static String TatolMatch(String str){
        String regex = "reviews: total:\\s+(.*?)\\s+";
        return DealStrSub.getSubUtil_2(str, regex);
    }

}
