package drpc;

import org.apache.storm.Config;
import org.apache.storm.utils.DRPCClient;

/**
 * Remote DRPC客户端测试类
 */
public class RemoteDRPCClient {
    public static void main(String[] args) throws Exception {
        Config config = new Config();
        config.put("storm.thrift.transport", "org.apache.storm.security.auth.SimpleTransportPlugin");
        config.put(Config.STORM_NIMBUS_RETRY_TIMES, 3);
        config.put(Config.STORM_NIMBUS_RETRY_INTERVAL, 18);
        config.put(Config.STORM_NIMBUS_RETRY_INTERVAL_CEILING, 20);
        config.put(Config.DRPC_MAX_BUFFER_SIZE, 1848576);

        DRPCClient client = new DRPCClient(config, "shizhan", 3772);
        String result = client.execute("addUser", "Tom");
        System.out.println("Client Invoked: " + result);
    }
}
