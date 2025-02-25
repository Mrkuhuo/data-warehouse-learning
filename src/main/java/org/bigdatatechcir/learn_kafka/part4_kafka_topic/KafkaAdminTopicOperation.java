package org.bigdatatechcir.learn_kafka.part4_kafka_topic;
import org.apache.kafka.clients.admin.*;

import java.util.*;
import java.util.concurrent.ExecutionException;
public class KafkaAdminTopicOperation {
    public static final String brokerList = "192.168.241.128:9092";
    public static final String topic = "topic-admin";

    public static void describeTopic(){
        String brokerList =  "localhost:9092";
        String topic = "topic-admin";

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
        AdminClient client = AdminClient.create(props);

        DescribeTopicsResult result = client.describeTopics(Collections.singleton(topic));
        try {
            Map<String, TopicDescription> descriptionMap =  result.all().get();
            System.out.println(descriptionMap.get(topic));
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        client.close();
    }

    public static void createTopic() {
        String brokerList =  "localhost:9092";
        String topic = "topic-admin";

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
        AdminClient client = AdminClient.create(props);

//        NewTopic newTopic = new NewTopic(topic, 4, (short) 1);

//        Map<String, String> configs = new HashMap<>();
//        configs.put("cleanup.policy", "compact");
//        newTopic.configs(configs);

        Map<Integer, List<Integer>> replicasAssignments = new HashMap<>();
        replicasAssignments.put(0, Arrays.asList(0));
        replicasAssignments.put(1, Arrays.asList(0));
        replicasAssignments.put(2, Arrays.asList(0));
        replicasAssignments.put(3, Arrays.asList(0));

        NewTopic newTopic = new NewTopic(topic, replicasAssignments);

        //代码清单4-4 可以从这里跟进去
        CreateTopicsResult result = client.
                createTopics(Collections.singleton(newTopic));
        try {
            result.all().get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        client.close();
    }

    public static void deleteTopic(){
        String brokerList =  "localhost:9092";
        String topic = "topic-admin";

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
        AdminClient client = AdminClient.create(props);

        try {
            client.deleteTopics(Collections.singleton(topic)).all().get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        client.close();
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
//        createTopic();
        describeTopic();
//        deleteTopic();

//        Properties props = new Properties();
//        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, brokerList);
//        props.put(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG, 30000);
//        AdminClient client = AdminClient.create(props);
////        createTopic(client);
////        deleteTopic(client);
//
//        NewTopic newTopic = new NewTopic(topic, 4, (short) 3);
//        Map<String, String> configs = new HashMap<>();
//        configs.put("cleanup.policy", "compact");
//        newTopic.configs(configs);
//        createTopic(client,newTopic);
    }

//    public static void deleteTopic(AdminClient client) throws ExecutionException, InterruptedException {
//        DeleteTopicsResult result = client.deleteTopics(Arrays.asList(topic));
//        result.all().get();
//    }
//
//    public static void createTopic(AdminClient client, NewTopic newTopic) throws ExecutionException,
//            InterruptedException {
//        CreateTopicsResult result = client.createTopics(Collections.singleton(newTopic));
//        result.all().get();
//    }
//
//    public static void createTopic(AdminClient client) throws ExecutionException, InterruptedException {
//        //创建一个主题：topic-demo，其中分区数为4，副本数为1
//        NewTopic newTopic = new NewTopic(topic, 4, (short) 1);
//        CreateTopicsResult result = client.createTopics(Collections.singleton(newTopic));
//        result.all().get();
//    }
}
