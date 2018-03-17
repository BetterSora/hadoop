package integration.jdbc;

import com.google.common.collect.Maps;
import org.apache.commons.io.FileUtils;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.jdbc.bolt.JdbcInsertBolt;
import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.jdbc.common.HikariCPConnectionProvider;
import org.apache.storm.jdbc.mapper.JdbcMapper;
import org.apache.storm.jdbc.mapper.SimpleJdbcMapper;
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

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * 使用storm完成词频统计分析，并写入到Mysql数据库中
 * @author Qin
 */
public class LocalWordCountJDBCStormTopology {

    public static class DataSourceSpout extends BaseRichSpout {
        private SpoutOutputCollector collector;

        @Override
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            this.collector = collector;
        }

        /**
         * 业务：
         * 1） 读取指定目录的文件夹下的数据
         * 2） 把每一行数据发射出去
         */
        @Override
        public void nextTuple() {
            // 获取.txt所有文件
            Collection<File> files = FileUtils.listFiles(new File("C:/Users/qinzhen/Desktop/输出/storm"),
                    new String[]{"txt"}, true);
            for (File file : files) {
                try {
                    // 获取文件中的所有内容
                    List<String> lines = FileUtils.readLines(file);
                    for (String line : lines) {
                        // 发射出去
                        collector.emit(new Values(line));
                    }

                    // 数据处理完之后，改名，否则一直重复执行
                    FileUtils.moveFile(file, new File(file.getAbsolutePath() + System.currentTimeMillis()));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("line"));
        }
    }

    /**
     * 对数据进行分割
     */
    public static class SplitBolt extends BaseRichBolt {
        private OutputCollector collector;

        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        /**
         * 业务逻辑：
         * line： 对line进行分割，按照逗号
         */
        @Override
        public void execute(Tuple input) {
            String line = input.getStringByField("line");
            String[] words = line.split(",");

            for (String word : words) {
                collector.emit(new Values(word));
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }
    }

    /**
     * 词频汇总Bolt
     */
    public static class CountBolt extends BaseRichBolt {
        private OutputCollector collector;

        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        Map<String, Integer> map = new HashMap<>();
        /**
         * 业务逻辑：
         * 1）获取每个单词
         * 2）对所有单词进行汇总
         * 3）输出
         */
        @Override
        public void execute(Tuple input) {
            String word = input.getStringByField("word");
            Integer count = map.get(word);
            if (count == null) {
                map.put(word, 1);
            } else {
                map.put(word, count+1);
            }

            this.collector.emit(new Values(word, map.get(word)));
            System.out.println("emit: " + word);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            // 要与mysql表中的字段一一对应
            declarer.declare(new Fields("word", "count"));
        }
    }

    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("DataSourceSpout", new DataSourceSpout());
        builder.setBolt("SplitBolt", new SplitBolt()).shuffleGrouping("DataSourceSpout");
        builder.setBolt("CountBolt", new CountBolt()).shuffleGrouping("SplitBolt");

        Map hikariConfigMap = Maps.newHashMap();
        hikariConfigMap.put("dataSourceClassName","com.mysql.jdbc.jdbc2.optional.MysqlDataSource");
        hikariConfigMap.put("dataSource.url", "jdbc:mysql://shizhan/storm");
        hikariConfigMap.put("dataSource.user","root");
        hikariConfigMap.put("dataSource.password","root");
        ConnectionProvider connectionProvider = new HikariCPConnectionProvider(hikariConfigMap);
        String tableName = "word_count";
        JdbcMapper simpleJdbcMapper = new SimpleJdbcMapper(tableName, connectionProvider);
        JdbcInsertBolt wordPersistanceBolt = new JdbcInsertBolt(connectionProvider, simpleJdbcMapper)
                .withTableName(tableName)
                .withQueryTimeoutSecs(30);
        builder.setBolt("wordPersistanceBolt", wordPersistanceBolt).shuffleGrouping("CountBolt");

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("LocalWordCountJDBCStormTopology", new Config(), builder.createTopology());
    }
}
