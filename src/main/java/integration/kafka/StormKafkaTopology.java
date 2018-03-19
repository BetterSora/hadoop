package integration.kafka;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;
import java.util.UUID;

/**
 * Storm消费kafka中数据
 * @author Qin
 */
public class StormKafkaTopology {
    public static class LogProcessBolt extends BaseRichBolt {
        private OutputCollector collector;

        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void execute(Tuple input) {
            try {
                // spout发出的fields为bytes
                byte[] bytes = input.getBinaryByField("bytes");
                String value = new String(bytes);
                System.out.println("value: " + value);

                collector.ack(input);
            } catch (Exception e) {
                collector.fail(input);
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {

        }
    }

    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();

        // Kafka使用的zk地址
        BrokerHosts hosts = new ZkHosts("shizhan:2181");

        // kafka存储数据的topic名称
        String topic = "project_topic";
        // 指定zk中的一个根目录，存储的是KafkaSpout读取数据的位置信息(offset)
        String zkRoot = "/" + topic;
        String id = UUID.randomUUID().toString();
        SpoutConfig config = new SpoutConfig(hosts, topic, zkRoot, id);
        // 设置读取偏移量的操作
        config.startOffsetTime = kafka.api.OffsetRequest.LatestTime();

        KafkaSpout kafkaSpout = new KafkaSpout(config);

        String SPOUT_ID = kafkaSpout.getClass().getSimpleName();
        builder.setSpout(SPOUT_ID, kafkaSpout);
        String BOLT_ID = LogProcessBolt.class.getSimpleName();
        builder.setBolt(BOLT_ID, new LogProcessBolt()).shuffleGrouping(SPOUT_ID);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(StormKafkaTopology.class.getSimpleName(), new Config(), builder.createTopology());
    }
}
