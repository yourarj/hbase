package mr.awesome.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.NamespaceExistException;
import org.apache.hadoop.hbase.client.Admin;

import java.io.IOException;

public class HBaseUtils {

    public static Configuration hbaseConfiguration(String zookeeperQuorum, String clientPort, String parentNode){
        Configuration configuration = HBaseConfiguration.create();
        configuration.set(HConstants.ZOOKEEPER_QUORUM, zookeeperQuorum);
        configuration.set(HConstants.ZOOKEEPER_CLIENT_PORT, clientPort);
        configuration.set(HConstants.ZOOKEEPER_ZNODE_PARENT, parentNode);
        return  configuration;
    }

    public static void createNamespace(Admin admin, String namespace){
        try {
            admin.createNamespace();
        } catch (NamespaceExistException e) {
            LOGGER.info("Namespace {} already exists! SKipping operation!");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
