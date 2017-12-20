package com.jnu.test;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class FirstTestForHBaseDelete {
    //创建配置实例
    public static Configuration conf = new HBaseConfiguration();

    public static void main(String[] args) throws IOException {
//        delete_1();
//        delete_2();
//        delete_4();
//        compareAndDelete();
    }


    /**
     * 使用原子性操作compare-and-delete删除值的简单示例
     * @throws IOException
     */
    private static void compareAndDelete() throws IOException {
        HTable table = new HTable(conf, "testtale");

        //创建一个Delete实例
        Delete delete1 = new Delete(Bytes.toBytes("row1"));
        delete1.deleteColumns(Bytes.toBytes("colfam1"),Bytes.toBytes("qual1"));

        //检查指定列是否不存在，根据检查结果执行删除操作
        boolean res1 = table.checkAndDelete(Bytes.toBytes("row1"),Bytes.toBytes("colfam2"),
                Bytes.toBytes("qual2"),null,delete1);
        //打印结果，结果为：
        System.out.println("Delete successful: " + res1);

        Delete delete2 = new Delete(Bytes.toBytes("row1"));
        //手工删除已经检查过的列
        delete2.deleteColumns(Bytes.toBytes("colfam2"),Bytes.toBytes("qual3"));
        table.delete(delete2);

        //尝试再一次删除同一个单元素
        boolean res2 = table.checkAndDelete(Bytes.toBytes("row1"),Bytes.toBytes("colfam2"),
                Bytes.toBytes("qual3"),null, delete1);
        //打印结果，结果因为：Delete successful： true。因为这个列之前存在所以成功删除
        System.out.println("Delete successful: " + res2);

        //创建另一个Delete实例，这次使用一个不同的行
        Delete delete3 = new Delete(Bytes.toBytes("row2"));
        delete3.deleteFamily(Bytes.toBytes("colfam1"));

        try{
            //检查这个不同的行，并执行删除操作
            boolean res4 = table.checkAndDelete(Bytes.toBytes("row1"),Bytes.toBytes("colfam1"),
                    Bytes.toBytes("qual1"),Bytes.toBytes("val1"),delete3);
            //执行不到这一行，在此行之前有异常抛出
            System.out.println("Delete successful: " + res4);
        }catch (Exception e){
            System.out.println("Error: " + e );
        }

    }
    /**
     * 从HBase中删除错误的数据
     * @throws IOException
     */
    private static void delete_4() throws IOException {
        HTable table = new HTable(conf, "testtable");

        List<Delete> deletes = new ArrayList<>();

        Delete delete = new Delete(Bytes.toBytes("row1"));
        //添加一个错误的列族来触发错误
        delete.deleteColumn(Bytes.toBytes("BOGUS"),Bytes.toBytes("qual1"));

        try{
            //从HBase表中删除数据
            table.delete(delete);
        }catch (Exception e){
            //捕获远程异常
            System.out.println("Erroe: " + e);
        }
        table.close();

        System.out.println("Delete length: " + deletes.size());
        for(Delete delete1 : deletes){
            //把失败的Delete操作打印出来，便于调试。
            System.out.println(delete1);
        }
    }

    /**
     * 删除指定列表的应用示例
     * @throws IOException
     */
    private static void delete_2() throws IOException {
        HTable table = new HTable(conf,"testtable");

        //创建一个列表，保存Delete实例
        List<Delete> deletes = new ArrayList<>();

        //为删除行的Delete实例设置时间戳
        Delete delete1 = new Delete(Bytes.toBytes("row1"));
        delete1.setTimestamp(4);
        deletes.add(delete1);

        Delete delete2 = new Delete(Bytes.toBytes("row2"));
        //删除一列的最新版本
        delete2.deleteColumn(Bytes.toBytes("colfam1"),Bytes.toBytes("qual1"));
        //在另一列中删除给定的版本以及所有更旧的版本
        delete2.deleteColumns(Bytes.toBytes("colfam2"),Bytes.toBytes("qual3"),3);
        deletes.add(delete2);

        Delete delete3 = new Delete(Bytes.toBytes("row3"));
        //删除整个列族，包括所有的列和版本
        delete3.deleteFamily(Bytes.toBytes("colfam1"));
        //在整个列族中，删除给定的版本以及更旧的版本。
        delete3.deleteFamily(Bytes.toBytes("colfam3"),3);
        deletes.add(delete3);

        //删除HBase表中的多行
        table.delete(deletes);

        table.close();


    }

    /**
     * 从HBase中删除数据的简单实例
     * @throws IOException
     */
    private static void delete_1() throws IOException {
        //实例化一个新的客户端
        HTable table = new HTable(conf, "testtable");

        //创建针对特定行的Delete实例
        Delete delete = new Delete(Bytes.toBytes("row1"));

        //设置时间戳
        delete.setTimestamp(1);

        //删除一列中的特定版本
        delete.deleteColumn(Bytes.toBytes("colfam1"),Bytes.toBytes("qual1"),1);

        //删除一列中的全部版本
        delete.deleteColumns(Bytes.toBytes("colfam2"),Bytes.toBytes("qual1"));
        //删除一列中的给定版本和所有更旧的版本
        delete.deleteColumns(Bytes.toBytes("colfam2"),Bytes.toBytes("qual3"),15);

        //删除整个列族，包括所有的列和版本。
        delete.deleteFamily(Bytes.toBytes("colfam3"));
        //删除给定列族中所有列的给定版本和所有更旧的版本。
        delete.deleteFamily(Bytes.toBytes("colfam3"),3);

        //从HBase表中删除数据
        table.delete(delete);

        table.close();
    }
}