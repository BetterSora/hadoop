package zk;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

public class ConfigWatcher implements Watcher {
    private ActiveKeyValueStore store = null;
    private final String PATH = "/zk";

    public ConfigWatcher(String address, int timeout) throws Exception {
        store = ActiveKeyValueStore.getConnection(address, timeout);
    }

    public void displayConfig() throws Exception {
        String result = store.read(PATH, this);
        System.out.println("Read " + PATH + " as " + result);
    }

    @Override
    public void process(WatchedEvent event) {
        if (event.getType() == Event.EventType.NodeDataChanged) {
            try {
                displayConfig();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws Exception {
        ConfigWatcher configWatcher = new ConfigWatcher("hadoop:2181", 3000);
        configWatcher.displayConfig();
        Thread.sleep(Long.MAX_VALUE);
    }
}
