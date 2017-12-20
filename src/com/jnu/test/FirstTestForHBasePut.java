package com.jnu.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Put的基本使用
 */
public class FirstTestForHBasePut {
    //创建所需的配置
    public static Configuration conf = HBaseConfiguration.create();

    public static void main(String[] args) throws IOException{
        //insert_1();
        //clientWriteBuffer();
        //insert_2();
        //insert_3();
        //insert_4();
        //compareAndSet();
    }

    /**
     * 使用原子性操作compare-and-set
     *
     * @throws IOException
     */
    private static void compareAndSet() throws IOException{
        HTable table = new HTable(conf, Bytes.toBytes("testtable"));
        //创建一个新的Put实例
        Put put1 = new Put(Bytes.toBytes("row15"));
        put1.add(Bytes.toBytes("colfam1"), Bytes.toBytes("quual1"), Bytes.toBytes("val1"));

        //检查指定列是否存在，按检查的结果决定是否执行put操作
        boolean res1  = table.checkAndPut(Bytes.toBytes("row15"),Bytes.toBytes("colfam1"),Bytes.toBytes("qual1"),null,put1);
        //输出结果，此处应为”Put applied： true“
        System.out.println("Put applied: " + res1);

        //再次向同一个单元格写入数据
        boolean res2 = table.checkAndPut(Bytes.toBytes("row15"),Bytes.toBytes("colfam1"),Bytes.toBytes("qual1"),null,put1);
        //因为那个列的值已经存在，此时的输出结果应为”Put applied:false“
        System.out.println("Put applied: " + res2);

        //创建一个新的put实例，这次使用一个不同的列限定符
        Put put2 = new Put(Bytes.toBytes("row15"));
        put2.add(Bytes.toBytes("colfam1"),Bytes.toBytes("qual2"),Bytes.toBytes("val2"));

        //当上一次的put值存在是，写入新的值
        boolean res3 = table.checkAndPut(Bytes.toBytes("row15"),Bytes.toBytes("colfam1"),Bytes.toBytes("qual1"),Bytes.toBytes("val1"),put2);
        //因为已经存在，所以输出的结构应为”Put applied: true“
        System.out.println("Put applid: " + res3);

        //再创建一个新的put实例，使用一个不同的行键
        Put put3 = new Put(Bytes.toBytes("row16"));
        put3.add(Bytes.toBytes("colfam1"),Bytes.toBytes("qual1"),Bytes.toBytes("val1"));

        //检查一个不同行的值是否相等，然后写入另一行，
        boolean res4 = table.checkAndPut(Bytes.toBytes("row15"),Bytes.toBytes("colfam1"),Bytes.toBytes("qual1"),Bytes.toBytes("val1"),put3);
        //程序执行不到这里，因为会在上一行代码中抛出异常。
        System.out.println("Put applied: " + res4);

    }

    /**
     * 向HBase中插入一个空的Put实例
     *
     */
    private static void put_4() throws IOException{
        HTable table = new HTable(conf, Bytes.toBytes("testtable"));
        List<Put> puts = new ArrayList<>();

        Put put1 = new Put(Bytes.toBytes("row11"));
        put1.add(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), Bytes.toBytes("val1"));
        puts.add(put1);

        Put put2 = new Put(Bytes.toBytes("row12"));
        put2.add(Bytes.toBytes("BOGUS"), Bytes.toBytes("qual1"), Bytes.toBytes("val1"));
        puts.add(put2);

        Put put3 = new Put(Bytes.toBytes("row13"));
        put3.add(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), Bytes.toBytes("val1"));
        puts.add(put3);

        Put put4 = new Put(Bytes.toBytes("row14"));
        //将没有内容的Put添加到列表中
        puts.add(put4);

        try{
            table.put(puts);
        }catch (Exception e){
            System.out.println("Error: " + e);
            //捕获本地异常，然后提交更新。
            table.flushCommits();
        }
    }

    /**
     * 向HBase中插入一个错误的列族，由于客户端不知道远程表的结构，因此对列族的检车会在服务器端完成。
     * 会报错org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException:
     * @throws IOException
     */
    private static void put_3() throws IOException{
        HTable table = new HTable(conf, "testtable");
        List<Put> puts = new ArrayList<>();
        Put put1 = new Put(Bytes.toBytes("row8"));
        put1.add(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), Bytes.toBytes("val1"));
        puts.add(put1);

        Put put2 = new Put(Bytes.toBytes("row9"));
        put2.add(Bytes.toBytes("BOGUS"),Bytes.toBytes("qual1"), Bytes.toBytes("val1"));
        //将使用不存在的列族的Put实例加入到列表
        puts.add(put2);

        Put put3 = new Put(Bytes.toBytes("row10"));
        put3.add(Bytes.toBytes("colfam1"), Bytes.toBytes("qual2"), Bytes.toBytes("val3"));
        puts.add(put3);

        //将多行多列数据存储到HBase中
        table.put(puts);
    }

    /**
     * 使用列表向HBase中添加数据
     * @throws IOException
     */
    private static void put_2() throws IOException{
        HTable table = new HTable(conf, "testtable");
        //创建一个列表，用于存放Put实例
        List<Put> puts = new ArrayList<>();

        Put put1 = new Put(Bytes.toBytes("row5"));
        put1.add(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), Bytes.toBytes("val1"));
        //将一个Put实例添加到列表中
        puts.add(put1);

        Put put2 = new Put(Bytes.toBytes("row6"));
        put2.add(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), Bytes.toBytes("val1"));
        //将另一个Put实例调价到列表中
        puts.add(put2);

        Put put3 = new Put(Bytes.toBytes("row7"));
        put3.add(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), Bytes.toBytes("val1"));
        //将第三个Put实例添加到列表中
        puts.add(put3);

        //向HBase中存入多行多列数据
        table.put(puts);
    }

    /**
     * 使用客户端写缓冲区的简单示例
     * @throws IOException
     */
    private static void clientWriteBuffer() throws IOException{

        HTable table = new HTable(conf, "testtable");
        //检查自动刷写标识位的设置，应该会打印出”auto flush： true“
        System.out.println("auto flush:" + table.isAutoFlush());
        //设置自动刷写为”false“，启用客户端写缓冲区
        table.setAutoFlush(false);
        Put put1 = new Put(Bytes.toBytes("row2"));
        put1.add(Bytes.toBytes("colfam1"),Bytes.toBytes("qual1"),Bytes.toBytes("val1"));
        //将一行和列数据存入Hbase
        table.put(put1);

        Put put2 = new Put(Bytes.toBytes("row3"));
        put2.add(Bytes.toBytes("colfam1"),Bytes.toBytes("qual1"),Bytes.toBytes("val2"));
        table.put(put2);

        Put put3 = new Put(Bytes.toBytes("row4"));
        put3.add(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), Bytes.toBytes("val3"));
        table.put(put3);

        Get get = new Get(Bytes.toBytes("row2"));
        Result result1 = table.get(get);
        //试图加载先前存储的行，结果会打印出”Result: keyvalues = NONE“
        System.out.println("Result: " + result1);
        //强制刷写缓冲区，会导致产生一个RPC请求
        table.flushCommits();

        Result result2 = table.get(get);
        //现在，这一行被持久化了，可以被读取到。
        System.out.println("Result: " + result2);
    }

    /**
     * 向HBase插入数据的一个简单示例
     * @throws IOException
     */
    private static void put_1() throws IOException{

        //实例化一个新的客户端
        HTable table = new HTable(conf, "testtable");
        //指定一行来创建一个Put，对该行进行插入新数据
        Put put = new Put(Bytes.toBytes("row1"));
        //向Put中添加一个名为"colfam1:qual1"的列
        put.add(Bytes.toBytes("colfam1"),Bytes.toBytes("qual1"),Bytes.toBytes("val1"));
        //向Put中添加另一个名为"colfam1:qual2"的列
        put.add(Bytes.toBytes("colfam1"),Bytes.toBytes("qual2"),Bytes.toBytes("val2"));
        //将这一行存储到HBase表中
        table.put(put);
    }
}
