CREATE CATALOG `paimon_hive_catalog` PROPERTIES (
    "type" = "paimon",
    "paimon.catalog.type" = "hms",
    "warehouse" = "hdfs:///user/hive/warehouse",
    "hive.metastore.uris" = "thrift://192.168.244.129:9083",
    "metastore.filter.hook" = "org.apache.hadoop.hive.metastore.DefaultMetaStoreFilterHookImpl",
    'hive.version' = '3.1.3'
);