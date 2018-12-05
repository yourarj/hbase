package mr.awesome.hbase.config;

import mr.awesome.hbase.HBaseUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.data.hadoop.hbase.HbaseTemplate;

import java.io.IOException;

@org.springframework.context.annotation.Configuration
public class HBaseConfig {
    public static final String COLUMN_FAMILY = "cf";
    public static final int COLUMN_FAMILY_TTL = 5000;
    public static final Logger LOGGER = LoggerFactory.getLogger(HBaseConfig.class)

    @Value("${app.hbase.config.client-port:2181}")
    private String clientPort;
    @Value("${app.hbase.config.zookeeper-quorum}")
    private String zookeeperQuorum;
    @Value("${app.hbase.config.parent-node:hbase-unsecure}")
    private String parentNode;

    @Bean
    private HbaseTemplate hbaseTemplate(){
        return new HbaseTemplate(getConfiguration());
    }

    public void setUpDatabse(){
        try {
            Connection connection = ConnectionFactory.createConnection(getConfiguration());
            Admin hbaseAdmin = connection.getAdmin();

            hbaseAdmin.creat
        } catch (IOException e) {
            LOGGER.error("Unable to create connection to HBase cluster and create table");
        }
    }

    private Configuration getConfiguration() {
        return HBaseUtils.hbaseConfiguration(zookeeperQuorum, clientPort, parentNode);
    }
}
