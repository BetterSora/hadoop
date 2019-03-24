package toolrunner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.Map;

/**
 * 测试Hadoop的工具：ToolRunner
 */
public class TestToolRunner extends Configured implements Tool {
    static {
        Configuration.addDefaultResource("hdfs-site.xml");
        // 只会加载classpath下的文件，这么写没用
        Configuration.addDefaultResource("/Users/qinzhen/software/hadoop/etc/hadoop/yarn-site.xml");
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        for (Map.Entry<String, String> entry : conf) {
            if ("color".equals(entry.getKey())) {
                System.out.println(entry.getValue());
            }
            if ("yarn.nodemanager.aux-services".equals(entry.getKey())) {
                System.out.println(entry.getValue());
            }
            //System.out.printf("%s=%s\n", entry.getKey(), entry.getValue());
        }

        return 0;
    }

    public static void main(String[] args) throws Exception {
        // 打印java classpath
        System.out.println(System.getProperty("java.class.path"));
        // 经过测试不会自动加载hdfs-site.xml文件
        Configuration conf = new Configuration();
        int exitCode = ToolRunner.run(conf, new TestToolRunner(), args);
        System.exit(exitCode);
    }
}
