set 'execution.checkpointing.interval'='21 s';

-- 样例1
EXECUTE JAR WITH (
'uri'='rs:/jar/flink/demo/SocketWindowWordCount.jar',
'main-class'='org.apache.flink.streaming.examples.socket',
'args'=' --hostname localhost ',
'parallelism'='',
);

-- 样例2
EXECUTE JAR WITH (
'uri'='rs:/paimon-flink-action-0.7.0-incubating.jar',
'main-class'='org.apache.paimon.flink.action.FlinkActions',
'args'='compact --warehouse hdfs:///tmp/paimon --database default --table use_be_hours_2'
);