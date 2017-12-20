package com.jnu.txtfile;

import org.jruby.RubyProcess;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Test {
    public static void main(String[] arags){
        String str = "title: World War II Allied Fighter Planes Trading Cards";
        String str1 = "   salesrank: ";
        String str2 = "2004-8-19  cutomer: A2591BUPXCS705  rating: 4  votes:   1  helpful:   12313";
        //Pattern pattern = Pattern.compile("[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}");
        String regexCutomer = "cutomer:\\s+(.*?)\\s+";
        String regexRating = "rating:\\s+(.*?)\\s+";
        String regexVote = "votes:\\s+([0-9]*)\\s+";
        String regexHelpful = "\\d+$";
        Pattern pattern = Pattern.compile(regexHelpful);
        Matcher matcher = pattern.matcher(str2);
        while(matcher.find()){
            System.out.println("Helpful: " + matcher.group());
        }
        //System.out.println("Helpful: " + DealStrSub.getSubUtil_2(str2,regexHelpful));
        System.out.println("Vote: " + DealStrSub.getSubUtil_2(str2, regexVote));
        System.out.println("Rating: " + DealStrSub.getSubUtil_2(str2, regexRating));
        System.out.println("Cutmer: " + DealStrSub.getSubUtil_2(str2, regexCutomer));

//        String str3 = "helpful:    342";
//        Pattern pattern = Pattern.compile("\\d+$");
//        Matcher matcher = pattern.matcher(str3);
//        //System.out.println(matcher.find());
//        while(matcher.find()){
//            System.out.println(matcher.group());
//        }
//
//        Matcher matcher = pattern.matcher(str2);
////        System.out.println(matcher.find());
////        System.out.println(matcher.group());
//        while(matcher.find()){
//            //System.out.println("ASIN: " + matcher.group());
//            System.out.println(matcher.group());
//        }
//        System.out.println(str1.length());
//        Pattern pattern = Pattern.compile(".*");
//        Matcher matcher = pattern.matcher(str);
//        while(matcher.find()){
//            System.out.println(matcher.group());
//        }

    }
}
