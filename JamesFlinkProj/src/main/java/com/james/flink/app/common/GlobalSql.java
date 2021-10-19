package com.james.flink.app.common;

/**
 * Created by James on 2021/10/18 上午7:55
 */
public class GlobalSql {
    /**
     * @param catalog
     * @param database
     * @param tableName
     * @return
     */
    public static String generateDataGenSourceTableSql(String catalog, String database, String tableName) {
        System.out.println(String.format("generateDataGenSourceTableSql() called, catalog=%s, database=%s, tableName=%s", catalog, database, tableName));

        return "CREATE TABLE IF NOT EXISTS " + catalog + "." + database + "." + tableName + " (\n" +
                " user_id int,\n" +
                " f_random_str STRING\n" +
                ") WITH (\n" +
                " 'connector' = 'datagen',\n" +
                " 'rows-per-second'='100',\n" +
                " 'fields.user_id.kind'='random',\n" +
                " 'fields.user_id.min'='1',\n" +
                " 'fields.user_id.max'='100',\n" +
                " 'fields.f_random_str.length'='10'\n" +
                ")";
    }

    /**
     * @param catalog
     * @param database
     * @param tableName
     * @return
     */
    public static String generatePrintSinkTableSql(String catalog, String database, String tableName) {
        System.out.println(String.format("generateDataGenSourceTableSql() called, catalog=%s, database=%s, tableName=%s", catalog, database, tableName));

        return "CREATE TABLE IF NOT EXISTS " + catalog + "." + database + "." + tableName + " (\n" +
                " user_id int,\n" +
                " f_random_str STRING\n" +
                ") WITH (\n" +
                " 'connector' = 'print'\n" +
                ")";
    }
}
