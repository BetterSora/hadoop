package zk;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * 随机更新zk的value
 */
public class ConfigUpdater {
    private ActiveKeyValueStore store = null;
    private final String PATH = "/zk";
    private Random random = new Random();

    public ConfigUpdater(String address, int timeout) {
        try {
            store = ActiveKeyValueStore.getConnection(address, timeout);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void run() throws Exception {
        while (true) {
            String value = random.nextInt(100) + "";
            store.write(PATH, value);
            System.out.println("set " + PATH + " to " + value);
            TimeUnit.SECONDS.sleep(random.nextInt(100));
        }
    }

    public static void main(String[] args) throws Exception {
        ConfigUpdater configUpdater = new ConfigUpdater("hadoop:2181", 3000);
        configUpdater.run();
    }

}
