package com.james.flink.app.iceberg;

import com.james.flink.app.common.GlobalSql;
import com.james.flink.utils.JamesUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;


/**
 * Created by James on 21-10-18 上午07:20
 */
public class FlinkTableWriteIcebergDemo {
    public static void main(String[] args) {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(10000);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String iceberg_catalog = "iceberg_catalog";
        String database = "iceberg_db_20211018";
        String hiveConfDir = "/home/james/install/apache-hive-3.1.2-bin/conf";
//        String hiveConfDir = "/home/james/install/hive-2.3.5/conf";

        String dataGenSourceTableName = "data_gen_source_table";
        String icebergTableName = "iceberg_table";
        String printSinkTableName = "print_sink_table";

        HiveCatalog hiveCatalog = new HiveCatalog(iceberg_catalog, null, hiveConfDir);
        tableEnv.registerCatalog(iceberg_catalog, hiveCatalog);

        tableEnv.useCatalog(iceberg_catalog);
        tableEnv.executeSql("CREATE DATABASE if not exists " + database);
        tableEnv.useDatabase(database);


        String createDataGenSrcTableSql = GlobalSql.generateDataGenSourceTableSql(iceberg_catalog, database, dataGenSourceTableName);
        JamesUtil.printDivider("createDataGenSrcTableSql");
        System.out.println(String.format("createDataGenSrcTableSql: %s", createDataGenSrcTableSql));
        tableEnv.executeSql(createDataGenSrcTableSql);

        /*
         * 打印自动生成源表的数据，验证时局是否自动生成
         */
//        tableEnv.executeSql(
//                String.format("SELECT user_id, f_random_str FROM %s.%s.%s", iceberg_catalog, database, srcTableName)).print();


        /*
         * 创建 Iceberg 表
         */
        tableEnv.executeSql(String.format("drop table if exists %s.%s.%s", iceberg_catalog, database, icebergTableName));


        String createIcebergTableSql = String.format("CREATE TABLE %s.%s.%s ( user_id int, f_random_str STRING) WITH ('connector' = 'iceberg', 'write.format.default' = 'ORC')", iceberg_catalog, database, icebergTableName);
        JamesUtil.printDivider("createIceberTableSql");
        System.out.println(String.format("createIceberTableSql: %s", createIcebergTableSql));
        tableEnv.executeSql(createIcebergTableSql);


        String insertSelectSql = String.format("INSERT INTO %s.%s.%s select * from %s", iceberg_catalog, database, icebergTableName, dataGenSourceTableName);
        JamesUtil.printDivider("insertSelectSql");
        System.out.println(String.format("insertSelectSql: %s", insertSelectSql));
        // Could not find any factory for identifier 'iceberg' that implements 'org.apache.flink.table.factories.DynamicTableFactory' in the classpath
        tableEnv.executeSql(insertSelectSql);


        /*
         * 创建 Print Sink 表
         */
        String createPrintSinkTableSql = GlobalSql.generatePrintSinkTableSql(iceberg_catalog, database, printSinkTableName);
        JamesUtil.printDivider("createPrintSinkTableSql");
        System.out.println(String.format("createPrintSinkTableSql: %s", createPrintSinkTableSql));
        tableEnv.executeSql(createPrintSinkTableSql);

        String insertIntoSinkTableSql = String.format("INSERT INTO %s.%s.%s select * from %s", iceberg_catalog, database, printSinkTableName, icebergTableName);
        JamesUtil.printDivider("insertIntoSinkTableSql");
        System.out.println(String.format("insertIntoSinkTableSql: %s", insertIntoSinkTableSql));
        tableEnv.executeSql(insertIntoSinkTableSql);
    }
}
