package com.jnu.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * 批量处理操作:批量处理跨行的不同操作
 */
public class FirstTestForHBaseRow {
    //创建一个配置实例
    private static Configuration conf = new HBaseConfiguration();

    private static byte[] ROW1 = Bytes.toBytes("row001");
    private static byte[] ROW2 = Bytes.toBytes("row002");
    private static byte[] COLFAM1 = Bytes.toBytes("colfam1");
    private static byte[] COLFAM2 = Bytes.toBytes("colfam2");
    private static byte[] QUAL1 = Bytes.toBytes("qual1");
    private static byte[] QUAL2 = Bytes.toBytes("qual2");


}
