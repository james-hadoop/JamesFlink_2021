package com.james.flink.app.table;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.source.FlinkSource;

/**
 * Created by James on 21-9-9 下午11:45
 */
public class FlinkTableReadSample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

        Configuration hadoopConf = new Configuration();

        TableLoader tableLoader = TableLoader.fromHadoopTable("hdfs://localhost:9000/user/hive/warehouse/iceberg/hadoop_catalog/iceberg_db_20211018/iceberg_table_dst", hadoopConf);

        DataStream<RowData> stream = FlinkSource.forRowData()
                .env(env)
                .tableLoader(tableLoader)
                .streaming(true)
                .build();

// Print all records to stdout.
        stream.print();

// Submit and execute this streaming read job.
        env.execute("Test Iceberg streaming Read");
    }
}
