package mr.awesome.hbase.config;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.Collections;

import static mr.awesome.hbase.utils.HBaseUtils.*;

@org.springframework.context.annotation.Configuration
public class HBaseConfig {
    public static final String NAMESPACE = "ns";
    public static final String COLUMN_FAMILY = "cf";
    public static final String TABLE_NAME = "tbl";
    public static final int COLUMN_FAMILY_TTL = 5000;
    public static final int COLUMN_FAMILY_MAX_VER = 1;
    public static final Logger LOGGER = LoggerFactory.getLogger(HBaseConfig.class);

    @Value("${app.hbase.config.client-port:2181}")
    private String clientPort;
    @Value("${app.hbase.config.zookeeper-quorum}")
    private String zookeeperQuorum;
    @Value("${app.hbase.config.parent-node:hbase-unsecure}")
    private String parentNode;

    @PostConstruct
    public void setUpDatabse(){
        try {
            Connection connection = ConnectionFactory.createConnection(getConfiguration());
            Admin hbaseAdmin = connection.getAdmin();

            createNamespace(hbaseAdmin, NAMESPACE);
            ColumnFamilyDescriptor columFamily = createColumFamily(COLUMN_FAMILY, COLUMN_FAMILY_TTL, COLUMN_FAMILY_MAX_VER);
            TableDescriptor tableDescriptor = createTableDescriptor(NAMESPACE, TABLE_NAME, Collections.singletonList(columFamily));
            createTable(hbaseAdmin, tableDescriptor);
        } catch (IOException e) {
            LOGGER.error("Unable to create connection to HBase cluster and create table", e);
        }
    }

    private Configuration getConfiguration() {
        return hbaseConfiguration(zookeeperQuorum, clientPort, parentNode);
    }
}
