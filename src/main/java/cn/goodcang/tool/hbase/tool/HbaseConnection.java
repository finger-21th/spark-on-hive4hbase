package cn.goodcang.tool.hbase.tool;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by jiandaohong on 2015/8/10.
 */

public class HbaseConnection {
    private static final Logger logger = LoggerFactory.getLogger(HbaseConnection.class);

    private Connection connection = null;
    private HbaseConfig config = null;

    public HbaseConnection() {
    }

    public synchronized int initConnection(HbaseConfig config) {
        if (null == config) {
            logger.error("config is null. cannot connect to hbase");
            return -1;
        }
        this.config = config;
        try {
            connection = ConnectionFactory.createConnection(config.getConfiguration());
        } catch (IOException e) {
            e.printStackTrace();
            return -1;
        }
        return 0;
    }

    public synchronized void releaseConnection() throws IOException {
        if (null != connection) {
            connection.close();
        }
        connection = null;
    }

    public synchronized Connection getConnection() {
        if (connection == null) {
            reconnect();
        }
        return connection;
    }

    public synchronized void reconnect() {
        try {
            if (null != connection) {
                connection.close();
            }
            connection = ConnectionFactory.createConnection(this.config.getConfiguration());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
