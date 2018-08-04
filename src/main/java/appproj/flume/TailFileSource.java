package appproj.flume;

import org.apache.commons.io.FileUtils;
import org.apache.flume.Context;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.Charset;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * 自定义source采集数据并保存偏移量
 * flume source 的生命周期：构造器 -> configure -> start -> processor.process
 * <p>
 * 1.读取配置文件: (配置文件的内容: 读取哪个文件、编码集、偏移量写到哪个文件、多长时间检测一下文件是否有新内容)
 */
public class TailFileSource extends AbstractSource implements EventDrivenSource, Configurable {
    private static final Logger logger = LoggerFactory.getLogger(TailFileSource.class);

    private String fileName;
    private String charset;
    private String positionFile;
    private long interval;
    private ExecutorService executor;
    private FileRunnable fileRunnable;

    @Override
    public void configure(Context context) {
        fileName = context.getString("fileName");
        charset = context.getString("charset", "UTF-8");
        positionFile = context.getString("positionFile");
        interval = context.getLong("interval", 1000L); // 默认1S一次
    }

    @Override
    public synchronized void start() {
        // 创建一个单线程的线程池
        executor = Executors.newSingleThreadExecutor();
        // 定义一个实现Runnable接口的类
        fileRunnable = new FileRunnable(fileName, positionFile, interval, charset, getChannelProcessor());
        // 将实现Runnable接口的类提交到线程池
        executor.submit(fileRunnable);
        // 调用父类的start方法
        super.start();
    }

    @Override
    public synchronized void stop() {
        fileRunnable.setFlag(false);
        executor.shutdown();

        while(!executor.isTerminated()) {
            logger.debug("Waiting for file executor service to stop");

            try {
                this.executor.awaitTermination(500L, TimeUnit.MILLISECONDS);
            } catch (InterruptedException var2) {
                logger.debug("Interrupted while waiting for exec executor service to stop. Just exiting.");
                Thread.currentThread().interrupt();
            }
        }
        super.stop();
    }

    private static class FileRunnable implements Runnable {
        private String charset;
        private long interval;
        private ChannelProcessor channelProcessor;
        private long offset;
        private RandomAccessFile raf;
        private boolean flag = true;
        private File pos;

        private FileRunnable(String fileName, String positionFile, long interval, String charset, ChannelProcessor channelProcessor) {
            this.charset = charset;
            this.interval = interval;
            this.channelProcessor = channelProcessor;

            // 读取偏移量，如果有，就接着读，没有就从头读
            pos = new File(positionFile);
            if (!pos.exists()) {
                // 不存在就创建一个位置文件
                try {
                    pos.createNewFile();
                } catch (IOException e) {
                    e.printStackTrace();
                    logger.error("create position file error", e);
                }
            }
            // 读取偏移量
            try {
                String offsetString = FileUtils.readFileToString(pos, Charset.defaultCharset());
                // 如果是第一次，是空或者空字符串
                if (offsetString != null && !"".equals(offsetString.trim())) {
                    // 将当前的偏移量转换成long
                    offset = Long.parseLong(offsetString);
                }
                // 读取log文件时从指定的位置读取数据
                raf = new RandomAccessFile(fileName, "r");
                // 按照指定的偏移量读取
                raf.seek(offset);

            } catch (IOException e) {
                e.printStackTrace();
                logger.error("read position file error", e);
            }
        }

        @Override
        public void run() {
            while (flag) {
                try {
                    // 每隔一段时间读数据
                    String line = raf.readLine();

                    if (line != null) {
                        // 解决RandomAccessFile中文乱码问题(默认使用ISO-8859-1)
                        line = new String(line.getBytes("ISO-8859-1"), Charset.forName(charset));
                        // 将数据发给channel
                        channelProcessor.processEvent(EventBuilder.withBody(line, Charset.forName(charset)));
                        // 获取最新的偏移量，然后更新偏移量
                        offset = raf.getFilePointer();
                        // 将偏移量写入到位置文件中(覆盖)
                        FileUtils.writeStringToFile(pos, offset + "", Charset.defaultCharset());
                    } else {
                        // 没有内容就睡一会
                        Thread.sleep(interval);
                    }

                } catch (InterruptedException e) {
                    e.printStackTrace();
                    logger.error("read file thread interrupted", e);
                } catch (IOException e) {
                    e.printStackTrace();
                    logger.error("read log file error", e);
                }
            }
        }

        private void setFlag(boolean flag) {
            this.flag = flag;
        }
    }
}
