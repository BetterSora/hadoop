package datacollect;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.TimerTask;

import org.apache.commons.io.FileUtils;

public class BackupCleanTask extends TimerTask {
	@Override
	public void run() {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd-HH");
		long now = new Date().getTime();
		try {
			Properties props = PropertyHolderLazy.getProps();

			// 探测本地备份目录
			File backupBaseDir = new File(props.getProperty(Constants.LOG_BACKUP_BASE_DIR));
			File[] dayBackDir = backupBaseDir.listFiles();

			// 判断备份日期子目录是否已超时
			for (File dir : dayBackDir) {
				long time = sdf.parse(dir.getName()).getTime();
				if(now - time > Integer.parseInt(props.getProperty(Constants.LOG_BACKUP_TIMEOUT)) * 60 * 60 * 1000L){
					FileUtils.deleteDirectory(dir);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
