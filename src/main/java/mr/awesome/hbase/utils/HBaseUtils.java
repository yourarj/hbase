package mr.awesome.hbase.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

/**
 * Common operations for HBase
 *
 * currently supports
 *  - creating configuration
 *  - creating namespace
 *  - creating creating colum family
 *  - creating table descriptor
 *  - creating table
 *
 * @author Mr. Arjun
 */
public class HBaseUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseUtils.class);

    /**
     * Constructs hbase configuration instance
     * @param zookeeperQuorum zookeeper quorum string
     * @param clientPort client port
     * @param parentNode parent node
     * @return Configuration
     */
    public static Configuration hbaseConfiguration(String zookeeperQuorum, String clientPort, String parentNode) {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set(HConstants.ZOOKEEPER_QUORUM, zookeeperQuorum);
        configuration.set(HConstants.ZOOKEEPER_CLIENT_PORT, clientPort);
        configuration.set(HConstants.ZOOKEEPER_ZNODE_PARENT, parentNode);
        return configuration;
    }

    /**
     * Attempts to create new Hbase namespace
     *
     * @param admin     Hbase Admin instance
     * @param namespace namespace to be created
     * @throws IOException if not able to create a namespace
     */
    public static void createNamespace(Admin admin, String namespace) throws IOException {
        try {
            admin.createNamespace(NamespaceDescriptor.create(namespace).build());
        } catch (NamespaceExistException e) {
            LOGGER.info("Namespace {} already exists! SKipping operation!");
        } catch (IOException e) {
            LOGGER.error("Unable to create Hbase namespace. {} error occurred", e.getMessage(), e);
            throw e;
        }
    }

    /**
     * Attempts to create new Hbase Table {@link HTable}
     * @param admin Hbase Admin instance
     * @param tableDescriptor {@link TableDescriptor}
     * @throws IOException if not able to create table
     */
    public static void createTable(Admin admin, TableDescriptor tableDescriptor) throws IOException {
        try {
            admin.createTable(tableDescriptor);
        } catch (TableExistsException tee) {
            LOGGER.info("Table {} already exists. Skipping creation!", tableDescriptor.getTableName().toString());
        } catch (IOException e) {
            LOGGER.error("Unable to create Hbase Table. {} error occurred", e.getMessage(), e);
            throw e;
        }

    }

    /**
     * Creates table descriptor instance {@link TableDescriptor}
     * @param namespace namespace of table to be created
     * @param tableName table name
     * @param columnFamilies list of column families {@link ColumnFamilyDescriptor}
     * @return TableDescriptor
     */
    public static TableDescriptor createTableDescriptor(String namespace, String tableName, List<ColumnFamilyDescriptor> columnFamilies) {
        return TableDescriptorBuilder
                .newBuilder(TableName.valueOf(namespace, tableName))
                .setColumnFamilies(columnFamilies)
                .build();
    }

    /**
     *  Creates column family {@link ColumnFamilyDescriptor}
     * @param columnFamily column family name
     * @param timeToLive time to live
     * @param maxVersions maximum number of versions to be maintained by hbase
     * @return ColumnFamilyDescriptor
     */
    public static ColumnFamilyDescriptor createColumFamily(String columnFamily, int timeToLive, int maxVersions) {
        return ColumnFamilyDescriptorBuilder
                .newBuilder(Bytes.toBytes(columnFamily))
                .setTimeToLive(timeToLive)
                .setMaxVersions(maxVersions)
                .build();
    }

    /**
     * Returns Charset analogous to provided string or UTF-8 otherwise
     * @param encoding encoding string
     * @return Charset
     */
    public static Charset getCharset(String encoding) {
        return (StringUtils.hasText(encoding) ? Charset.forName(encoding) : Charset.forName("UTF-8"));
    }
}
