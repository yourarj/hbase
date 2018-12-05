package mr.awesome.hbase.service;

import org.springframework.data.hadoop.hbase.HbaseTemplate;

public class DbStoreHbase implements DbStore {

    private HbaseTemplate hBaseTemplate;

    public DbStoreHbase(HbaseTemplate hBaseTemplate) {
        this.hBaseTemplate = hBaseTemplate;
    }

    @Override
    public void fetchResult() {
        //hBaseTemplate.find()

    }

    @Override
    public void putResult() {

    }
}
