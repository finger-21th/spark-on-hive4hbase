package cn.goodcang.tool.hbase.tool;

import cn.goodcang.tool.hbase.common.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * Created by jiandaohong on 2015/8/10.
 */

public class HbaseAdapter {
    private static final Logger logger = LoggerFactory.getLogger(HbaseAdapter.class);

    private HbaseConnectionPool connectionPool = null;
    private static HbaseAdapter hbaseAdapter = null;

    private HbaseAdapter() {
    }

    public static HbaseAdapter getInstance() {
        if (null == hbaseAdapter) {
            synchronized (HbaseAdapter.class) {
                if (null == hbaseAdapter) {
                    hbaseAdapter = new HbaseAdapter();
                }
            }
        }
        return hbaseAdapter;
    }

    public int init(String zk_host) {
        int poolSize = Constants.HBASE_CONNECTION_POOLSIZE;
        int waittime = Constants.HBASE_CONNECTION_WAITTIME_MILLIS;

        // create hbase configuration by hase-site.xml
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", zk_host);

        HbaseConfig hbaseConfig = new HbaseConfig(poolSize,
                waittime, configuration);

        connectionPool = new HbaseConnectionPool();
        int ret = connectionPool.init(hbaseConfig);
        if (0 != ret) {
            logger.error("init connectionPool failed");
            return -1;
        }
        return 0;
    }

    public void clearPool() {
        connectionPool.clearPool();
    }


    /*
     * 创建表
     */
    public void createTable(String tableName) {
        logger.info("begin to create table :" + tableName);
        HbaseConnection conn = null;
        try {
            conn = connectionPool.getConnection();
            if (null == conn) {
                logger.warn("get conn from pool failed. cannot create table");
                return;
            }
            Admin admin = conn.getConnection().getAdmin();
            TableName tabName = TableName.valueOf(tableName);
            if (admin.tableExists(tabName)) {
                admin.disableTable(tabName);
                admin.deleteTable(tabName);
                System.out.println(tabName + " is exist, delete.....");
            }
            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
            admin.createTable(tableDescriptor);
            admin.close();
        } catch (MasterNotRunningException e) {
            e.printStackTrace();
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (conn != null) {
                connectionPool.releaseConnection(conn);
            }
        }
        logger.info("create table success");
    }

    /*
     * 删除表
     */
    public void dropTable(String tableName) {
        HbaseConnection conn = null;
        try {
            conn = connectionPool.getConnection();
            if (null == conn) {
                logger.warn("get conn from pool failed");
                return;
            }
            Admin admin = conn.getConnection().getAdmin();
            TableName tabName = TableName.valueOf(tableName);
            if (admin.tableExists(tabName)) {
                admin.disableTable(tabName);
                admin.deleteTable(tabName);
                admin.close();
                admin = null;
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (conn != null) {
                connectionPool.releaseConnection(conn);
            }
        }
    }

    /*
     * 判断表是否存在
     */
    public boolean isExist(String tableName) {
        boolean retValue = false;
        HbaseConnection conn = null;
        try {
            conn = connectionPool.getConnection();
            if (null == conn) {
                logger.warn("get conn from pool failed");
                return false;
            }
            Admin admin = conn.getConnection().getAdmin();
            TableName tabName = TableName.valueOf(tableName);
            if (admin.tableExists(tabName)) {
                retValue = true;
            }
            admin.close();
        } catch (IOException e) {
            logger.warn("exception:" + e.getMessage());
        } finally {
            if (conn != null) {
                connectionPool.releaseConnection(conn);
            }
        }
        return retValue;
    }

    /*
     * 根据rowkey删除某一行数据，删除整行的所有列簇、所有行、所有版本
     * 单行删除，避免使用
     */
    public void deleteRowByRowKey(String tableName, String rowkey) {
        HbaseConnection conn = null;
        try {
            conn = connectionPool.getConnection();
            if (null == conn) {
                logger.error("get connection from pool failed");
                return;
            }
            TableName tabName = TableName.valueOf(tableName);
            Table table = conn.getConnection().getTable(tabName);
            Delete del = new Delete(Bytes.toBytes(rowkey));
            table.delete(del);
            table.close();
            table = null;
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (conn != null) {
                connectionPool.releaseConnection(conn);
            }
        }
    }

    /*
     * 多行同时删除
     */
    public void multiDeleteRowByRowKey(String tableName, String[] rowkeys) {
        int rowNum = rowkeys.length;
        HbaseConnection conn = null;
        try {
            conn = connectionPool.getConnection();
            if (null == conn) {
                logger.error("get connection from pool failed");
                return;
            }
            TableName tabName = TableName.valueOf(tableName);
            Table table = conn.getConnection().getTable(tabName);
            List<Delete> list = new ArrayList<Delete>();
            for (int i = 0; i < rowNum; ++i) {
                Delete del = new Delete(Bytes.toBytes(rowkeys[i]));
                list.add(del);
                table.delete(list);
                table.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (conn != null) {
                connectionPool.releaseConnection(conn);
            }
        }
    }

    public List<Cell> getRowByRowKey(String tableName, List<String> fields, String rowkey) {

        List<Cell> cells = null;
        HbaseConnection connection = null;
        try {
            connection = connectionPool.getConnection();

            Table table = connection.getConnection().getTable(TableName.valueOf(tableName));

            if (rowkey != null) {
                Get get = new Get(Bytes.toBytes(rowkey));

                fields.forEach(new Consumer<String>() {
                    @Override
                    public void accept(String field) {
                        get.addColumn(Bytes.toBytes(Constants.COLUMN_FAMILY), Bytes.toBytes(field));
                    }
                });

                cells = table.get(get).listCells();
                table.close();
                table = null;
            }
            return cells;
        } catch (IOException e) {
            e.printStackTrace();
            return cells;
        } finally {
            if (connection != null) {
                connectionPool.releaseConnection(connection);
            }
        }
    }

    /*
     * 根据rowkey获取单行数据，包括所有列簇、所有列的最新版本数据
     * 当行获取，避免使用此函数
     */
    public Result getRowByRowKey(String tableName, String rowkey) {
        Result result = null;
        HbaseConnection conn = null;
        try {
            TableName tabName = TableName.valueOf(tableName);
            conn = connectionPool.getConnection();
            if (null == conn) {
                logger.error("get connection from pool failed");
                return null;
            }
            Table table = conn.getConnection().getTable(tabName);
            Admin admin = conn.getConnection().getAdmin();
            if (!admin.isTableEnabled(tabName)) {
                logger.error("table " + tableName + " in hbase is not enable");
            } else {
                Get get = new Get(rowkey.getBytes());
                result = new Result();
                result = table.get(get);
            }
            table.close();
            admin.close();
            table = null;
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("hbase get exception" + e.getMessage());
            return result;
        } finally {
            if (conn != null) {
                connectionPool.releaseConnection(conn);
            }
        }
        return result;
    }

    /*
     * 多行同时获取
     */
    public List<Result> multiGetRowByRowKey(String tableName, String[] rowkeys) {
        int rowNum = rowkeys.length;
        if (0 == rowNum) {
            return null;
        }
        List<Result> retList = new ArrayList<Result>();
        HbaseConnection conn = null;
        try {
            conn = connectionPool.getConnection();
            if (null == conn) {
                logger.warn("get connection from pool failed");
                return null;
            }
            TableName tabName = TableName.valueOf(tableName);
            Table table = conn.getConnection().getTable(tabName);
            Admin admin = conn.getConnection().getAdmin();
            if (!admin.isTableEnabled(tabName)) {
                logger.error("table " + tableName + " in hbase is not enable");
                retList = null;
            } else {
                List list = new ArrayList();
                for (int i = 0; i < rowNum; ++i) {
                    Get get = new Get(Bytes.toBytes(rowkeys[i]));
                    list.add(get);
                }
                Result[] resluts = table.get(list);
                for (int i = 0; i < resluts.length; ++i) {
                    retList.add(resluts[i]);
                }
            }
            table.close();
            admin.close();
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("hbase multi get exception" + e.getMessage());
            return null;
        } finally {
            if (conn != null) {
                connectionPool.releaseConnection(conn);
            }
        }
        return retList;
    }

    /*
     * 新增和更新共用put，如果行键存在，则更新；否则新增
     * 单条数据插入或更新，避免调用
     */
    public int putRowByRowKey(String tableName, String rowkey, String columnFamily, String columm, String value) {
        int ret = 0;
        HbaseConnection conn = null;
        try {
            conn = connectionPool.getConnection();
            if (null == conn) {
                logger.warn("get connection from pool failed");
                return -1;
            }
            TableName tabName = TableName.valueOf(tableName);
            Table table = conn.getConnection().getTable(tabName);
            Admin admin = conn.getConnection().getAdmin();
            if (!admin.isTableEnabled(tabName)) {
                logger.error("table " + tableName + " in hbase is not enable");
                ret = -1;
            } else {
                Put put = new Put(Bytes.toBytes(rowkey));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columm), Bytes.toBytes(value));
                table.put(put);
            }
            table.close();
            admin.close();
            table = null;
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("hbase put exception:" + e.getMessage());
            return -1;
        } finally {
            if (conn != null) {
                connectionPool.releaseConnection(conn);
            }
        }
        return ret;
    }

    /*
     * 为某一个列簇所有列添加值
     */
    public int multiPutRowByRowKey(String tableName, String rowkey,
                                   String columnFamily, String[] columns, String[] values) {

        int colNum = columns.length;
        int valNum = values.length;
        if (colNum != valNum) {
            return -1;
        }
        int ret = 0;
        HbaseConnection conn = null;
        try {
            conn = connectionPool.getConnection();
            if (null == conn) {
                logger.warn("get connection from pool failed");
                return -1;
            }
            TableName tabName = TableName.valueOf(tableName);
            Table table = conn.getConnection().getTable(tabName);
            Admin admin = conn.getConnection().getAdmin();
            if (!admin.isTableEnabled(tabName)) {
                logger.error("table " + tableName + " in hbase is not enable");
                ret = -1;
            } else {
                Put put = new Put(Bytes.toBytes(rowkey));
                for (int i = 0; i < colNum; ++i) {
                    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columns[i]), Bytes.toBytes(values[i]));
                }
                table.put(put);
            }
            table.close();
            admin.close();
            table = null;
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("hbase multi put exception." + e.getMessage());
            return -1;
        } finally {
            if (conn != null) {
                connectionPool.releaseConnection(conn);
            }
        }
        return ret;
    }

    /*
     * 为多行，某一个列簇某一列添加值
     */
    public int multiPutRowsByRowKey(String tableName,
                                    String columnFamily,
                                    final String column,
                                    final String[] rowkeys,
                                    final String[] valueList) {
        int ret = 0;
        int keySize = rowkeys.length;
        int listSize = valueList.length;
        if (keySize != listSize) {
            logger.warn("rowkeys size not equal to value list size");
            return -1;
        }
        HbaseConnection conn = null;
        try {
            conn = connectionPool.getConnection();
            if (null == conn) {
                logger.warn("get connection from pool failed");
                return -1;
            }
            TableName tabName = TableName.valueOf(tableName);
            Table table = conn.getConnection().getTable(tabName);
            Admin admin = conn.getConnection().getAdmin();
            if (!admin.isTableEnabled(tabName)) {
                logger.error("table " + tableName + " in hbase is not enable");
                ret = -1;
            } else {
                List<Put> putList = new ArrayList<Put>();
                for (int i = 0; i < keySize; ++i) {
                    Put put = new Put(Bytes.toBytes(rowkeys[i]));
                    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(valueList[i]));
                    putList.add(put);
                }
                table.put(putList);
            }
            table.close();
            admin.close();
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("hbase multiput exception." + e.getMessage());
        } finally {
            if (conn != null) {
                connectionPool.releaseConnection(conn);
            }
        }
        return ret;
    }

    public int multiPut(String tableName, List<Put> putList) {
        HbaseConnection conn = null;
        try {
            conn = connectionPool.getConnection();
            if (null == conn) {
                logger.warn("get connection from pool failed");
                return -1;
            }
            TableName tabName = TableName.valueOf(tableName);
            Admin admin = conn.getConnection().getAdmin();
            Table table = conn.getConnection().getTable(tabName);
            if (!admin.isTableEnabled(tabName)) {
                logger.error("table " + tableName + " in hbase is not enable");
            } else {
                table.put(putList);
            }
            table.close();
            admin.close();
            table = null;
        } catch (IOException e) {
            e.printStackTrace();
            return -1;
        } finally {
            if (conn != null) {
                connectionPool.releaseConnection(conn);
            }
        }
        return 0;
    }

    public List<Result> scanRowByRange(String tableName, String beginRow, String endRow) {
        List<Result> list = new ArrayList<Result>();
        ResultScanner results = null;
        HbaseConnection conn = null;
        try {
            conn = connectionPool.getConnection();
            if (null == conn) {
                logger.error("get connection from pool failed");
                return null;
            }
            TableName tabName = TableName.valueOf(tableName);
            Table table = conn.getConnection().getTable(tabName);
            Admin admin = conn.getConnection().getAdmin();
            if (!admin.isTableEnabled(tabName)) {
                logger.error("table " + tableName + " in hbase is not enable");
                list = null;
            } else {
                Scan scan = new Scan();
                scan.setStartRow(Bytes.toBytes(beginRow));
                scan.setStopRow(Bytes.toBytes(endRow + 0)); // 包括改行
                results = table.getScanner(scan);

                for (Result result : results) {
                    list.add(result);
                }
                results.close();
            }
            table.close();
            admin.close();
            table = null;
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("hbase scan exception. " + e.getMessage());
            return null;
        } finally {
            if (conn != null) {
                connectionPool.releaseConnection(conn);
            }
        }
        return list;
    }

    public List<Result> scanRowByRange(String tableName, String columnFamily, String column,
                                       String beginRow, String endRow) {
        List<Result> list = new ArrayList<Result>();
        ResultScanner results = null;
        HbaseConnection conn = null;
        try {
            conn = connectionPool.getConnection();
            if (null == conn) {
                logger.error("get connection from pool failed");
                return null;
            }
            TableName tabName = TableName.valueOf(tableName);
            Table table = conn.getConnection().getTable(tabName);
            Admin admin = conn.getConnection().getAdmin();
            if (!admin.isTableEnabled(tabName)) {
                logger.error("table " + tableName + " in hbase is not enable");
                list = null;
            } else {
                Scan scan = new Scan();
                scan.addFamily(Bytes.toBytes(columnFamily));
                // scan.addColumn(Bytes.toBytes(column), Bytes.toBytes(column));
                scan.setStartRow(Bytes.toBytes(beginRow));
                scan.setStopRow(Bytes.toBytes(endRow + 0)); // 包括该行
                long begin = System.currentTimeMillis();
                results = table.getScanner(scan);
                long end = System.currentTimeMillis();
                if (null != results) {
                    for (Result result : results) {
                        list.add(result);
                    }
                    results.close();
                } else {
                    logger.warn("scan from hbase result is null");
                }
                logger.info("hbase table getScanner success. result size: " + list.size() + " cost:" + (end - begin) + "ms");
            }
            table.close();
            admin.close();
            table = null;
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (conn != null) {
                connectionPool.releaseConnection(conn);
            }
        }
        return list;
    }
}
