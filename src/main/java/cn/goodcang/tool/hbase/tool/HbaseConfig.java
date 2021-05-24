package cn.goodcang.tool.hbase.tool;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by jiandaohong on 2015/8/10.
 */

/*
 * hbase configuration wrapper
 */

public class HbaseConfig {
    private static final Logger logger = LoggerFactory.getLogger(HbaseConfig.class);

    protected Configuration configuration = null;
    private int poolSize;
    private int waitTimeMillis;

    public HbaseConfig(int poolSize, int waitTimeMillis, Configuration configuration) {
        this.poolSize = poolSize;
        this.waitTimeMillis = waitTimeMillis;
        this.configuration = configuration;
    }

    public Configuration getConfiguration() { return configuration; }
    public int getPoolSize() { return poolSize; }
    public int getWaitTimeMillis() { return waitTimeMillis; }
}
