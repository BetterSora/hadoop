import java.util.Arrays;

public class Test3 {
    public static void main(String[] args) {
        String str = "babcdfrr";

        int maxCount = 0;
        String res = "";
        int count = 0;
        char[] chars = str.toCharArray();
        One:
        for (int i = 0; i < chars.length; i = i + count) {
            count = 0;
            Two:
            for (int j = i; j < chars.length; j++) {
                if (chars[i] == chars[j]) {
                    count++;
                } else {
                    if (count > maxCount) {
                        maxCount = count;
                        res = new String(chars).substring(i, j + 1);
                    } else if (count == maxCount) {
                        String temp = new String(chars).substring(i, j + 1);
                        if ("".equals(temp)) {
                            break;
                        }
                        String[] temp2 = {temp, res};
                        Arrays.sort(temp2);
                        res = temp2[0];
                    }
                    break Two;
                }

                if (j == chars.length - 1 && count > maxCount) {
                    res = new String(chars).substring(i, j + 1);
                    break One;
                } else if (j == chars.length - 1 && count == maxCount) {
                    String temp = new String(chars).substring(i, j + 1);
                    if ("".equals(temp)) {
                        break;
                    }
                    String[] temp2 = {temp, res};
                    Arrays.sort(temp2);
                    res = temp2[0];
                    break One;
                }
            }
        }

        //System.out.println(maxCount);
        System.out.println(res);
    }
}