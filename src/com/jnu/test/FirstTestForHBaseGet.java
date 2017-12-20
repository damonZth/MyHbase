package com.jnu.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * get方法的使用
 * get方法分为两类： 1、一次获取一行的数据；2、一次获取多行
 *
 */
public class FirstTestForHBaseGet {
    //创建配置实例
   public static Configuration conf = new HBaseConfiguration();

   public static void main(String[] args) throws IOException {
       //get_1();
       //get_2();
       //get_3();
       get_4();
   }

    /**
     * 使用特殊检索方法
     * 使用getRowOrBefore()方法时，需要指定一个已经存在的列族，否则服务端会因为要访问一个不存在的存储文件二抛出一个空指针错误。
     *
     * @throws IOException
     */
   private static void get_4() throws IOException{
        HTable table = new HTable(conf, "testtable");

        //尝试查找已经存在的行
        Result result = table.getRowOrBefore(Bytes.toBytes("row1"), Bytes.toBytes("colfam1"));
        //打印查找结果
        System.out.println("Found: " + Bytes.toString(result.getRow()));

        //尝试查找不存在的行
        Result result1 = table.getRowOrBefore(Bytes.toBytes("row99"),Bytes.toBytes("colfam1"));
        //返回已排好序的表中的最后一条结果
        System.out.println("Found: " + Bytes.toString(result1.getRow()));

        //打印返回结果
        for(KeyValue kv : result1.raw()){
            System.out.println("Col: " + Bytes.toString(kv.getFamily()) +
                    "/" + Bytes.toString(kv.getQualifier()) +
                    ",Value: " + Bytes.toString(kv.getValue()));
        }

        //尝试查找测试行之前的一行
        Result result2 = table.getRowOrBefore(Bytes.toBytes("abc"),Bytes.toBytes("colfam1"));
       //由于没有匹配的结果，返回null
        System.out.println("Found: " + result2);

   }

    /**
     * 读取一个错误的列族
     * 抛出异常： org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException
     * @throws IOException
     */
   private static void get_3() throws IOException{
       HTable table = new HTable(conf, "testtable");

       //准备共用的字节数组
       byte[] cf1 = Bytes.toBytes("colfam1");
       byte[] qf1 = Bytes.toBytes("qual1");
       byte[] qf2 = Bytes.toBytes("qual2");
       byte[] row1 = Bytes.toBytes("row1");
       byte[] row2 = Bytes.toBytes("row2");

       List<Get> gets = new ArrayList<>();

       //将Get实例添加到列表中
       Get get1 = new Get(row1);
       get1.addColumn(cf1, qf1);
       gets.add(get1);

       Get get2 = new Get(row2);
       get2.addColumn(cf1, qf2);
       gets.add(get2);

       Get get3 = new Get(row2);
       get2.addColumn(cf1, qf2);
       gets.add(get3);

       //添加包含错误的列族的Get实例
       Get get4 = new Get(row2);
       get4.addColumn(Bytes.toBytes("BOGUS"),qf2);
       gets.add(get4);

       //抛出异常，操作停止
       Result[] results = table.get(gets);
       //不会执行到此
       System.out.println("Result count: " + results.length);

   }

    /**
     * 使用Get实例的列表从HBase中获取数据
     *
     * @throws IOException
     */
   private static void get_2() throws IOException{
       HTable table = new HTable(conf, "testtable");

       //准备共用的字节数组
       byte[] cf1 = Bytes.toBytes("colfam1");
       byte[] qf1 = Bytes.toBytes("qual1");
       byte[] qf2 = Bytes.toBytes("qual2");
       byte[] row1 = Bytes.toBytes("row1");
       byte[] row2 = Bytes.toBytes("row2");

       //准备存放Get实例的列表
       List<Get> gets = new ArrayList<>();

       //将Get实例存放到列表中
       Get get1 = new Get(row1);
       get1.addColumn(cf1, qf1);
       gets.add(get1);

       Get get2 = new Get(row2);
       get2.addColumn(cf1, qf1);
       gets.add(get2);

       Get get3 = new Get(row2);
       get3.addColumn(cf1, qf2);
       gets.add(get3);

       //从HBase中获取这些行和选定的列
       Result[] results = table.get(gets);

       //遍历结果病检查哪些行中包含选定的列
       System.out.println("First iteration...");
       for(Result result : results){
           String row = Bytes.toString(result.getRow());
           System.out.print("Row: " + row + " ");
           byte[] val = null;
           if(result.containsColumn(cf1, qf1)){
               val = result.getValue(cf1, qf1);
               System.out.println("Value: " + Bytes.toString(val));
           }
           if(result.containsColumn(cf1, qf2)){
               val = result.getValue(cf1, qf2);
               System.out.println("Value: " + Bytes.toString(val));
           }
       }

       //再次遍历，病打印所有的结果
       System.out.println("Second iterator...");
       for(Result result : results){
           for(KeyValue kv : result.raw()){
               System.out.println("Row: " + Bytes.toString(kv.getRow()) + " Value: " + Bytes.toString(kv.getValue()));
           }
       }

   }

    /**
     * 从HBase中获取数据的完整过程
     * 这个例子只添加并取回了一个特定的列，取回的版本数为默认值1.
     * get()方法调用后返回一个Result类的实例。
     * @throws IOException
     */
   private static void get_1() throws IOException{
       //初始化一个新的表的引用
       HTable table = new HTable(conf, "testtable");
       //使用一个指定的行键构建一个Get实例
       Get get = new Get(Bytes.toBytes("row1"));
       //向Get实例中添加一个列
       get.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"));
       //从HBase中获取指定列的行数据
       Result result = table.get(get);
       //从返回结果中获取对应列的数据
       byte[] val = result.getValue(Bytes.toBytes("colfam1"),Bytes.toBytes("qual1"));
       //将数据转化为字符串打印输出
       System.out.println("Value: " + Bytes.toString(val));
   }

}
