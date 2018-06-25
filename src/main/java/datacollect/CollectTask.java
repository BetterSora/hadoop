package datacollect;

import java.io.File;
import java.io.FilenameFilter;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Properties;
import java.util.TimerTask;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class CollectTask extends TimerTask {
	/**
	 * ——定时探测日志源目录
	 * ——获取需要采集的文件
	 * ——移动这些文件到一个待上传临时目录
	 * ——遍历待上传目录中各文件，逐一传输到HDFS的目标路径，同时将传输完成的文件移动到备份目录
	 */
	@Override
	public void run() {
		try {
			// 获取配置参数
			Properties props = PropertyHolderLazy.getProps();

			// 获取本次采集时的日期
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd-HH");
			String day = sdf.format(new Date());

			File srcDir = new File(props.getProperty(Constants.LOG_SOURCE_DIR));

			// 列出日志源目录中需要采集的文件
			File[] listFiles = srcDir.listFiles((File dir, String name) -> name.startsWith(props.getProperty(Constants.LOG_LEGAL_PREFIX)));

			System.out.println("探测到如下文件需要采集：" + Arrays.toString(listFiles));

			// 将要采集的文件移动到待上传临时目录
			File toUploadDir = new File(props.getProperty(Constants.LOG_TOUPLOAD_DIR));
			for (File file : listFiles) {
				FileUtils.moveFileToDirectory(file, toUploadDir, true);
			}

			System.out.println("上述文件移动到了待上传目录" + toUploadDir.getAbsolutePath());

			// 构造一个HDFS的客户端对象
			FileSystem fs = FileSystem.get(new URI(props.getProperty(Constants.HDFS_URI)), new Configuration(), "root");
			File[] toUploadFiles = toUploadDir.listFiles();

			// 检查HDFS中的日期目录是否存在，如果不存在，则创建
			Path hdfsDestPath = new Path(props.getProperty(Constants.HDFS_DEST_BASE_DIR) + day);
			if (!fs.exists(hdfsDestPath)) {
				fs.mkdirs(hdfsDestPath);
			}

			// 本地的备份目录
			File backupDir = new File(props.getProperty(Constants.LOG_BACKUP_BASE_DIR) + day + "/");

			for (File file : toUploadFiles) {
				// 传输文件到HDFS并改名access_log_
				Path destPath = new Path(hdfsDestPath + "/" + props.getProperty(Constants.HDFS_FILE_PREFIX) + UUID.randomUUID() + props.getProperty(Constants.HDFS_FILE_SUFFIX));
				fs.copyFromLocalFile(new Path(file.getAbsolutePath()), destPath);

				System.out.println("文件传输到HDFS完成：" + file.getAbsolutePath() + "-->" + destPath);

				// 将传输完成的文件移动到备份目录
				FileUtils.moveFileToDirectory(file, backupDir, true);

				System.out.println("文件备份完成：" + file.getAbsolutePath() + "-->" + backupDir);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
