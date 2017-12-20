package com.jnu.txtfile;

import org.apache.hadoop.hbase.util.ByteBloomFilter;
import org.jruby.RubyProcess;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 使用NIO按行读写大文件
 */
public class NIOTxt {
    public static void main(String[] args) throws IOException{
        int bufSize = 10000000;
        File fin = new File("/home/damon/amazon-meta.txt");
        File fout = new File("/home/damon/amazon-test011.txt");

        Date startDate = new Date();

        FileChannel fcin = new RandomAccessFile(fin, "r").getChannel();
        ByteBuffer readBuffer = ByteBuffer.allocate(bufSize);

        FileChannel fcout = new RandomAccessFile(fout, "rws").getChannel();
        ByteBuffer writeBuffer = ByteBuffer.allocateDirect(bufSize);

        //readFileByLine(bufSize, fcin, readBuffer, fcout, writeBuffer);
        //readFileByLine_1(bufSize, fcin, readBuffer);
        readFileByLine_1(bufSize, fcin, readBuffer,fcout,writeBuffer);

        //readFileByLineStr(bufSize,fcin,readBuffer);

        Date endDate = new Date();

        System.out.println(startDate + "|" + endDate);
        if(fcin.isOpen()){
            fcin.close();
        }
        if(fcout.isOpen()){
            fcout.close();
        }

    }
    private static void writeFileByLine_1(FileChannel fcout, ByteBuffer writeBuffer, String line){
        try{
            fcout.write(writeBuffer.wrap(line.getBytes()),fcout.size());
        }catch (IOException e){
            e.printStackTrace();
        }
    }
    private static void readFileByLine_1(int bufSize, FileChannel fcin, ByteBuffer readBuffer,
                                         FileChannel fcout, ByteBuffer writeBuffer){
        String enterStr = "\n";
        String line = null;
        String newline = null;
        try{
            byte[] bs = new byte[bufSize];

            int size = 0;
            StringBuffer strBuf = new StringBuffer("");
            while(fcin.read(readBuffer) != -1){
                int readSize = readBuffer.position();
                readBuffer.rewind();
                readBuffer.get(bs);
                readBuffer.clear();
                String tempStr = new String(bs, 0, readSize);

                int fromIndex = 0;
                int endIndex = 0;

                while((endIndex = tempStr.indexOf(enterStr, fromIndex)) != -1){

                    line = tempStr.substring(fromIndex, endIndex);
                    line = new String(strBuf.toString() + line);

                    if (!(line.contains("similar") || line.contains("categories") || line.contains("|"))) {
                        newline = line;
                        // System.out.println(newline);
                        writeFileByLine_1(fcout,writeBuffer,newline + "\n");

                    }


                    strBuf.delete(0, strBuf.length());
                    fromIndex = endIndex + 1;
                    //System.out.print(line);

                }
                //System.out.println();
                //System.out.println(new String(stringBuffer));

                if(readSize > tempStr.length()){
                    strBuf.append(tempStr.substring(fromIndex, tempStr.length()));
                }else{
                    strBuf.append(tempStr.substring(fromIndex, readSize));
                }
            }
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    private static void readFileByLineStr(int bufSize, FileChannel fcin, ByteBuffer readBuffer){
        String enterStr = "Id:";
        String line = null;
        try{
            byte[] bs = new byte[bufSize];

            int size = 0;
            StringBuffer strBuf = new StringBuffer("");
            while(fcin.read(readBuffer) != -1){
                int readSize = readBuffer.position();
                readBuffer.rewind();
                readBuffer.get(bs);
                readBuffer.clear();
                String tempStr = new String(bs, 0, readSize);

                int fromIndex = 0;
                int endIndex = 0;

                while((endIndex = tempStr.indexOf(enterStr, fromIndex)) != -1){

                    line = tempStr.substring(fromIndex, endIndex);
                    line = new String(strBuf.toString() + line);
                    System.out.println("段开始: " + line + " 段结束");
                    strBuf.delete(0, strBuf.length());
                    fromIndex = endIndex + 1;
                }
                if(readSize > tempStr.length()){
                    strBuf.append(tempStr.substring(fromIndex, tempStr.length()));
                }else{
                    strBuf.append(tempStr.substring(fromIndex, readSize));
                }
            }
        }catch (IOException e) {
            e.printStackTrace();
        }
    }


    private static void readFileByLine_1(int bufSize, FileChannel fcin, ByteBuffer readBuffer){
        String enterStr = "\n";
        String line = null;
        String newline = null;
        try{
            byte[] bs = new byte[bufSize];

            int size = 0;
            StringBuffer strBuf = new StringBuffer("");
            while(fcin.read(readBuffer) != -1){
                int readSize = readBuffer.position();
                readBuffer.rewind();
                readBuffer.get(bs);
                readBuffer.clear();
                String tempStr = new String(bs, 0, readSize);

                int fromIndex = 0;
                int endIndex = 0;

                while((endIndex = tempStr.indexOf(enterStr, fromIndex)) != -1){

                    line = tempStr.substring(fromIndex, endIndex);
                    line = new String(strBuf.toString() + line);


                    //writeFileByLine(fcout,writeBuffer,line);

                    strBuf.delete(0, strBuf.length());
                    fromIndex = endIndex + 1;
                    //System.out.print(line);


                    if (!(line.contains("similar") || line.contains("categories") || line.contains("|"))) {
                        //System.out.print(line);
                        //System.out.println(IdToIdMatch(line));
                        if (line.contains("Id:")) {
                            //stringBuffer.append("Id: " + IdMatch(line) + "\t|\t");
                            System.out.print("Id: " + IdMatch(line) + "\t");
                        }
                        if (line.contains("ASIN:")) {
                            //stringBuffer.append("ASIN: " + ASINMatch(line) + "\t|\t");
                            System.out.print("ASIN: " + ASINMatch(line) + "\t");
                        }
                        if (line.contains("title:")) {
//                          int titleLen = line.length();
//                          System.out.println("titleLen: " + titleLen);
//                          System.out.println(line);
//                          System.out.println("Title: " + line.substring(9));
                            //stringBuffer.append("Title: " + TitleMatch(line) + "\t|\t");
                            System.out.print("Title: " + TitleMatch(line) + "\t");
                        }
                        if(line.contains("group:")){
                            //stringBuffer.append("Group: " + GroupMatch(line) + "\t|\t");
                            System.out.print("Group: " + GroupMatch(line) + "\t");
                        }
                        if(line.contains("salesrank:")){
                            //stringBuffer.append("SalesRank: " + SalesrankMatch(line) + "\t|\t");
                            System.out.print("SalesRank: " + SalesrankMatch(line) + "\t");
                        }
                        System.out.println();

//
//                        int count = 0;
//                        if(line.contains("reviews:")){
//                            count = Integer.parseInt(TatolMatch(line));
//                            System.out.print("Count: " + count);
//                        }
//
                        if(line.contains("cutomer:")){
//                            //StringBuffer stringBuffer2 = new StringBuffer();
//                            //stringBuffer2.append("Time: " + TimeMatch(line) + "\t|\t" );
//                            //stringBuffer2.append("Cutomer: " + CutomerMatch(line) + "\t|\t" );
//                            //stringBuffer2.append("Rating: " + RatingMatch(line) + "\t|\t" );
//                            //stringBuffer2.append("Voting: " + VoteMatch(line) + "\t|\t" );
//                            //stringBuffer2.append("Helpful: " + HelpfulMatch(line) + "\t|\t" );
//                            //stringBuffer.append(stringBuffer2 + "\n");
                            System.out.print("Time: " + TimeMatch(line) + "\t");
                            System.out.print("Cutomer: " + CutomerMatch(line) + "\t");
                            System.out.print("Rating: " + RatingMatch(line) + "\t");
                            System.out.print("Votes: " + VoteMatch(line) + "\t");
                            System.out.print("Helpful " + HelpfulMatch(line));
                        }
                        //System.out.println();

                        //System.out.println(new String(stringBuffer1));
                    }

                }
                //System.out.println();
                //System.out.println(new String(stringBuffer));

                if(readSize > tempStr.length()){
                    strBuf.append(tempStr.substring(fromIndex, tempStr.length()));
                }else{
                    strBuf.append(tempStr.substring(fromIndex, readSize));
                }
            }
        }catch (IOException e){
            e.printStackTrace();
        }
    }
    private static void readFileByLine(int bufSize, FileChannel fcin, ByteBuffer readBuffer){
        String enter = "\n";
        //存储读取的每行数据
        //List<String> dataList = new ArrayList<>();
        byte[] lineByte = new byte[0];

        String encode = "GBK";
        //String encode = "UTF-8";

        String line = null;
        String newline = null;

        try {
            byte[] temp = new byte[0];
            while(fcin.read(readBuffer) != -1){
                int readSize = readBuffer.position();//读取结束后的位置,相当于读取的长度
                byte[] bs = new byte[readSize];//用来存放读取的内容的数组
                readBuffer.rewind();//将position设置回0
                //相当于readBuffer.get(bs, 0, bs.length()):从position初始位置开始相对读,读取bs.length个byte,
                //并写入bs[0]到bs[bs,length-1]的区域
                readBuffer.get(bs);
                readBuffer.clear();

                int startNum = 0;
                int LF = 10;//换行符
                int CR = 13;//回车符
                boolean hasLF = false;//判断是否有换行符
                for(int i = 0; i < readSize; i++){
                    if(bs[i] == LF){
                        hasLF = true;
                        int tempNum = temp.length;
                        int lineNum = i - startNum;
                        lineByte = new byte[tempNum + lineNum];//数组大小已经去掉换行符
                        System.arraycopy(temp, 0, lineByte, 0, tempNum);//填充lineByte的0到tempNum-1
                        temp = new byte[0];
                        System.arraycopy(bs, startNum, lineByte, tempNum, lineNum);//填充lineByte的tempNum到tempNum+lineNum-1

                        //读取的一行,转换为String类型
                        line = new String(lineByte, 0, lineByte.length, encode);//一行完整的字符串,过滤了换行和回车


                        //dataList.add(line);
                        //打印输出
                        //System.out.println(line);

                        //writeFileByLine(fcout, writeBuffer,line,enter);

                        if(i + 1 < readSize && bs[i + 1] == CR){
                            startNum  = i + 2;
                        }else{
                            startNum = i + 1;
                        }
                    }
                }

                if(hasLF){
                    temp = new byte[bs.length - startNum];
                    System.arraycopy(bs, startNum, temp, 0, temp.length);
                }else {//兼容单次读取的内容不足一行的情况
                    byte[] toTemp = new byte[temp.length + bs.length];
                    System.arraycopy(temp, 0, toTemp, 0, temp.length);
                    System.arraycopy(bs, 0, toTemp, temp.length, bs.length);
                    temp = toTemp;
                }
            }
//            System.out.println(line);
//            String newline;
//            /**
//             * 这里做一个字符匹配,对每一行进行处理
//             * id,asin,title,group,salesrank,reviews(time,cutomer,rating,votes,helpful)等10个属性
//             *
//             */
//            if(line.contains("Id")){
//                newline = line;
//            }
            //兼容文件最后一行没有换行的情况
            if(temp != null && temp.length > 0){
                //读取的一行转换为String类型
                line = new String(temp,0,temp.length,encode);
                //dataList.add(line);
                //打印输出
                System.out.println(line);

                //writeFileByLine(fcout,writeBuffer,line,enter);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * 读取文件
     * @param bufSize
     * @param fcin
     * @param readBuffer
     * @param fcout
     * @param writeBuffer
     */
    private static void readFileByLine(int bufSize, FileChannel fcin, ByteBuffer readBuffer,
                                       FileChannel fcout, ByteBuffer writeBuffer){
        String enter = "\n";
        //存储读取的每行数据
        List<String> dataList = new ArrayList<>();
        byte[] lineByte = new byte[0];

        String encode = "GBK";
        //String encode = "UTF-8";

        try {
            byte[] temp = new byte[0];
            while(fcin.read(readBuffer) != -1){
                int readSize = readBuffer.position();//读取结束后的位置,相当于读取的长度
                byte[] bs = new byte[readSize];//用来存放读取的内容的数组
                readBuffer.rewind();//将position设置回0
                //相当于readBuffer.get(bs, 0, bs.length()):从position初始位置开始相对读,读取bs.length个byte,
                //并写入bs[0]到bs[bs,length-1]的区域
                readBuffer.get(bs);
                readBuffer.clear();

                int startNum = 0;
                int LF = 10;//换行符
                int CR = 13;//回车符
                boolean hasLF = false;//判断是否有换行符
                for(int i = 0; i < readSize; i++){
                    if(bs[i] == LF){
                        hasLF = true;
                        int tempNum = temp.length;
                        int lineNum = i - startNum;
                        lineByte = new byte[tempNum + lineNum];//数组大小已经去掉换行符
                        System.arraycopy(temp, 0, lineByte, 0, tempNum);//填充lineByte的0到tempNum-1
                        temp = new byte[0];
                        System.arraycopy(bs, startNum, lineByte, tempNum, lineNum);//填充lineByte的tempNum到tempNum+lineNum-1
                        String line = new String(lineByte, 0, lineByte.length, encode);//一行完整的字符串,过滤了换行和回车
                        dataList.add(line);
                        System.out.println(line);

                        writeFileByLine(fcout, writeBuffer,line,enter);

                        if(i + 1 < readSize && bs[i + 1] == CR){
                            startNum  = i + 2;
                        }else{
                            startNum = i + 1;
                        }
                    }
                }
                if(hasLF){
                    temp = new byte[bs.length - startNum];
                    System.arraycopy(bs, startNum, temp, 0, temp.length);
                }else {//兼容单次读取的内容不足一行的情况
                    byte[] toTemp = new byte[temp.length + bs.length];
                    System.arraycopy(temp, 0, toTemp, 0, temp.length);
                    System.arraycopy(bs, 0, toTemp, temp.length, bs.length);
                    temp = toTemp;
                }
            }
            //兼容文件最后一行没有换行的情况
            if(temp != null && temp.length > 0){
                String line = new String(temp,0,temp.length,encode);
                dataList.add(line);
                System.out.println(line);

                writeFileByLine(fcout,writeBuffer,line,enter);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 写数据到文件上
     * @param fcout
     * @param writeBuffer
     * @param line
     * @param enter
     */
    private static void writeFileByLine(FileChannel fcout, ByteBuffer writeBuffer, String line, String enter){
        try{
            fcout.write(writeBuffer.wrap(line.getBytes()),fcout.size());
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    /**
     * 根据id提取文本中的id值
     * @param str
     */
    private static String IdMatch(String str){
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
    private static String ASINMatch(String str){
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
    private static String TitleMatch(String str){
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
    private static String GroupMatch(String str){
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
    private static String SalesrankMatch(String str){
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
    private static String TimeMatch(String str){
        String reStr = "";
        Pattern pattern = Pattern.compile("[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}");
        Matcher matcher = pattern.matcher(str);
        while(matcher.find()){
            //System.out.println("ASIN: " + matcher.group());
            reStr = matcher.group();
        }
        return reStr;
    }

    private static String CutomerMatch(String str){
        String regex = "cutomer:\\s+(.*?)\\s+rating";
        return DealStrSub.getSubUtil_2(str,regex);
    }

    private static String RatingMatch(String str){
        String regex = "rating:\\s+(.*?)\\s+votes";
        return DealStrSub.getSubUtil_2(str,regex);
    }

    private static String VoteMatch(String str){
        String regex = "votes:\\s+(.*?)\\s+";
        return DealStrSub.getSubUtil_2(str, regex);
    }

    private static String HelpfulMatch(String str){
        return DealStrSub.getLastNum(str);
    }

    private static String TatolMatch(String str){
        String regex = "reviews: total:\\s+(.*?)\\s+";
        return DealStrSub.getSubUtil_2(str, regex);
    }
    private static String IdToIdMatch(String str){
        String regex = "Id:\\s+(.*?)Id:\\s+";
        return DealStrSub.getSubUtil_2(str, regex);
    }

}

