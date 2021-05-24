package cn.goodcang.tool.hbase.tool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


/**
 * Created by jiandaohong on 2015/8/10.
 */

/*
 * hbase connection pool for a hbase cluster
 */

public class HbaseConnectionPool {
    private static final Logger logger = LoggerFactory.getLogger(HbaseConnectionPool.class);

    private List<HbaseConnection> busyConnection;
    private List<HbaseConnection> idleConnection;
    // 集群配置
    private HbaseConfig hbaseClusterConfig = null;


    public int init(HbaseConfig hbaseConfig) {
        if (hbaseConfig == null) {
            return -1;
        }
        idleConnection = new LinkedList<HbaseConnection>();
        busyConnection = new LinkedList<HbaseConnection>();
        this.hbaseClusterConfig = hbaseConfig;

        int ret;
        HbaseConnection connection;
        for (int i = 0; i < hbaseConfig.getPoolSize(); ++i) {
            connection = new HbaseConnection();
            ret = connection.initConnection(hbaseConfig);
            if (0 != ret) {
                logger.warn("init connection failed.");
                return -1;
            }
            idleConnection.add(connection);
            logger.debug("add connection success");
        }

        return 0;
    }

    // TODO
    public void clearPool() {
        try {
            int size = idleConnection.size();
            for (HbaseConnection connection : idleConnection) {
                if (connection != null) {
                    connection.getConnection().close();
                }
            }
            for (HbaseConnection connection : busyConnection) {
                if (connection != null) {
                    connection.getConnection().close();
                }
            }
            busyConnection.clear();
            idleConnection.clear();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void resetConnectionPool() {
        idleConnection = new LinkedList<HbaseConnection>();
        busyConnection = new LinkedList<HbaseConnection>();
        HbaseConnection connection;
        int ret = 0;
        for (int i = 0; i < hbaseClusterConfig.getPoolSize(); ++i) {
            connection = new HbaseConnection();
            ret = connection.initConnection(hbaseClusterConfig);
            if (0 != ret) {
                logger.warn("init connection failed.");
            }
            idleConnection.add(connection);
            logger.debug("add connection success");
        }
    }

    public HbaseConnection getConnection() {
        if (idleConnection.isEmpty()) {
            logger.warn("no idle connection");
            return null;
        }

        logger.debug("get connection. before idle pool :" + idleConnection.size());
        HbaseConnection connection;
        connection = idleConnection.get(0);
        idleConnection.remove(connection);
        busyConnection.add(connection);
        logger.debug("get connection. after idle pool :" + idleConnection.size());
        logger.debug("get connection from pool success");
        return connection;
    }

    public synchronized void releaseConnection(HbaseConnection connection) {
        logger.debug("before busy size :" + busyConnection.size() + "idle size :" + idleConnection.size());
        idleConnection.add(connection);
        busyConnection.remove(connection);
        logger.debug("after busy size :" + busyConnection.size() + "idle size :" + idleConnection.size());
        logger.debug("release connection success");
    }

}
