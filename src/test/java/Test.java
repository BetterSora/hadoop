import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.HashMap;

public class Test {
    public static void main(String[] args) throws Exception {
        HashMap<String, Integer> map = new HashMap<>();
        map.put("1", 0);

        Integer num = map.get("2");
        System.out.println(num);

        new BufferedReader(new FileReader(new File("")));
    }
}
