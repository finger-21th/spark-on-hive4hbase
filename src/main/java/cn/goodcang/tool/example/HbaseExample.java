package cn.goodcang.tool.example;


import cn.goodcang.tool.hbase.tool.HbaseAdapter;

/**
 * Created by jiandaohong on 2015/11/27.
 */

public class HbaseExample {
    public static void main(String[] args) {
        // init hbase connection pool
        HbaseAdapter hbaseAdapter = HbaseAdapter.getInstance();
        hbaseAdapter.init("192.168.87.13:2181,192.168.87.153:2181,192.168.87.7:2181");

        // use hbase Adapter function
        System.out.println(hbaseAdapter.isExist("ods_order_operation_time"));
        hbaseAdapter.clearPool();
        // or you can wrap a HbaseProxy class to use HbaseAdapter
    }
}
