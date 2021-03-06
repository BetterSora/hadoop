package storm;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.List;
import java.util.Map;

/**
 * storm本地求和案列
 * fieldsGrouping测试
 * @author Qin
 */
public class LocalSumStormFieldsGroupingTopology {
    /**
     * Spout需要继承BaseRichSpout
     * 数据源需要产生数据并发射
     */
    public static class DataSourceSpout extends BaseRichSpout {
        private SpoutOutputCollector collector;

        /**
         * 初始化方法，只会被调用一次
         * @param conf 配置参数
         * @param context 上下文
         * @param collector 数据发射器
         */
        @Override
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            this.collector = collector;
        }

        int number = 0;
        /**
         * 会产生数据，在生产上从消息队列中获取数据
         * 这个方法是一个死循环，会一直不停的执行
         */
        @Override
        public void nextTuple() {
            List<Integer> taskId = collector.emit(new Values(number % 2, ++number));
            System.out.println(taskId);
            System.out.println("Spout：" + number);

            // 防止数据产生太快
            Utils.sleep(1000);
        }

        /**
         * 声明输出字段
         * @param declarer 用于输出字段
         */
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            // 和上面的nextTuple方法发射的number对应，上面只有一个，下面也只能定义一个
            declarer.declare(new Fields("flag", "num"));
        }
    }

    /**
     * 数据的累计求和Bolt：接收数据并处理
     */
    public static class SumBolt extends BaseRichBolt {
        /**
         * 初始化方法，会被执行一次
         * @param stormConf 配置参数
         * @param context 上下文
         * @param collector 数据发射器
         */
        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

        }

        int sum = 0;
        /**
         * 死循环，获取Spout发送过来的数据
         * @param input 数据
         */
        @Override
        public void execute(Tuple input) {
            // Bolt中获取值可以根据index获取，也可以根据上一个环节中定义的field名称获取
            Integer value = input.getIntegerByField("num");
            sum += value;

            System.out.println("Bolt：sum = [" + sum + "]");
            System.out.println(Thread.currentThread().getName() + "---" + value);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {

        }
    }

    public static void main(String[] args) {
        // TopologyBuilder根据Spout和Bolt来构建出Topology
        // Storm中任何一个作业都是通过Topology的方式进行提交的
        // Topology中需要指定Spout和Bolt的执行顺序
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("DataSourceSpout", new DataSourceSpout());
        builder.setBolt("SumBolt", new SumBolt(), 3).setNumTasks(6)
                .fieldsGrouping("DataSourceSpout", new Fields("flag"));

        // 创建一个本地Storm集群：本地模式运行，不需要搭建Storm集群
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("LocalSumStormFieldsGroupingTopology", new Config(), builder.createTopology());
    }
}
