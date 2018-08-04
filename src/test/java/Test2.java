import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class Test2 {
    public static void main(String arg[]){
        String str = "ABCABCdd";
        char[] chars = str.toCharArray();

        HashMap<String, Integer> map = new HashMap<>();

        for (int i = 0; i < chars.length; i++) {
            String key = chars[i] + "";
            if (map.containsKey(key)) {
                map.put(key, map.get(key) + 1);
            } else {
                map.put(key, 1);
            }
        }

        Set<Map.Entry<String, Integer>> entries = map.entrySet();
        StringBuilder sb = new StringBuilder();

        for (Map.Entry<String, Integer> entry : entries) {
            if (entry.getValue() >= 2) {
                sb.append(entry.getKey());
            }
        }

        char[] result = sb.toString().toCharArray();
        Arrays.sort(result);

        System.out.println(new String(result));
    }
}
