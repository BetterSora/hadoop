package appproj;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

public class AppLogDataClean {

    public static class AppLogDataCleanMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
        Text k = null;
        NullWritable v = null;
        MultipleOutputs<Text, NullWritable> mos = null;  //多路输出器

        @Override
        protected void setup(Context context) {
            k = new Text();
            v = NullWritable.get();
            mos = new MultipleOutputs<>(context);
        }


        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            JSONObject jsonObj = JSON.parseObject(value.toString());
            JSONObject headerObj = jsonObj.getJSONObject(GlobalConstants.HEADER);

            // 过滤缺失必选字段的记录
            if (StringUtils.isBlank(headerObj.getString(GlobalConstants.SDK_VER))) {
                return;
            }

            if (StringUtils.isBlank(headerObj.getString(GlobalConstants.TIME_ZONE))) {
                return;
            }

            if (StringUtils.isBlank(headerObj.getString(GlobalConstants.COMMIT_ID))) {
                return;
            }

            if (StringUtils.isBlank(headerObj.getString(GlobalConstants.COMMIT_TIME))) {
                return;
            }

            if (StringUtils.isBlank(headerObj.getString(GlobalConstants.PID))) {
                return;
            }

            if (StringUtils.isBlank(headerObj.getString(GlobalConstants.APP_TOKEN))) {
                return;
            }

            if (StringUtils.isBlank(headerObj.getString(GlobalConstants.APP_ID))) {
                return;
            }

            if (StringUtils.isBlank(headerObj.getString(GlobalConstants.DEVICE_ID))) {
                return;
            }

            if (StringUtils.isBlank(headerObj.getString(GlobalConstants.DEVICE_ID_TYPE))) {
                return;
            }

            if (StringUtils.isBlank(headerObj.getString(GlobalConstants.RELEASE_CHANNEL))) {
                return;
            }

            if (StringUtils.isBlank(headerObj.getString(GlobalConstants.APP_VER_NAME))) {
                return;
            }

            if (StringUtils.isBlank(headerObj.getString(GlobalConstants.APP_VER_CODE))) {
                return;
            }

            if (StringUtils.isBlank(headerObj.getString(GlobalConstants.OS_NAME))) {
                return;
            }

            if (StringUtils.isBlank(headerObj.getString(GlobalConstants.OS_VER))) {
                return;
            }

            if (StringUtils.isBlank(headerObj.getString(GlobalConstants.LANGUAGE))) {
                return;
            }

            if (StringUtils.isBlank(headerObj.getString(GlobalConstants.COUNTYR))) {
                return;
            }

            if (StringUtils.isBlank(headerObj.getString(GlobalConstants.MANUFACTURE))) {
                return;
            }

            if (StringUtils.isBlank(headerObj.getString(GlobalConstants.DEVICE_MODEL))) {
                return;
            }

            if (StringUtils.isBlank(headerObj.getString(GlobalConstants.RESOLUTION))) {
                return;
            }

            if (StringUtils.isBlank(headerObj.getString(GlobalConstants.NET_TYPE))) {
                return;
            }

            // 生成user_id
            String user_id = "";
            if ("android".equals(headerObj.getString(GlobalConstants.OS_NAME).trim())) {
                user_id = StringUtils.isNotBlank(headerObj.getString(GlobalConstants.ANDROID_ID)) ? headerObj.getString(GlobalConstants.ANDROID_ID)
                        : headerObj.getString(GlobalConstants.DEVICE_ID);
            } else {
                user_id = headerObj.getString(GlobalConstants.DEVICE_ID);
            }

            // 输出结果，添加user_id字段
            headerObj.put("user_id", user_id);
            k.set(JsonToStringUtil.toString(headerObj));

            if ("android".equals(headerObj.getString(GlobalConstants.OS_NAME))) {
                // android/目录下前缀为android
                mos.write(k, v, "android/android");
            } else {
                mos.write(k, v, "ios/ios");
            }

        }


        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            mos.close();
        }

    }

    public static void main(String[] args) throws Exception {
        // 在代码中设置JVM系统参数，用于给job对象来获取访问HDFS的用户身份
        System.setProperty("HADOOP_USER_NAME", "qinzhen");

        Configuration conf = new Configuration();
        // 1、设置job运行时要访问的默认文件系统
        conf.set("fs.defaultFS", "hdfs://hadoop:8020");
        // 2、设置job提交到哪去运行
        conf.set("mapreduce.framework.name", "yarn");
        conf.set("yarn.resourcemanager.hostname", "hadoop");
        // 3、如果要从windows系统上运行这个job提交客户端程序，则需要加这个跨平台提交的参数
        conf.set("mapreduce.app-submission.cross-platform","true");

        Job job = Job.getInstance(conf);
        job.setJar("d:/app.jar");
        job.setMapperClass(AppLogDataCleanMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setNumReduceTasks(0);

        // 避免生成默认的part-m-00000等文件，因为，数据已经交给MultipleOutputs输出了
        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean res = job.waitForCompletion(true);
        System.exit(res ? 0 : 1);
    }
}