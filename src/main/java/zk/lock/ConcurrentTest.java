package zk.lock;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;

/**
 * 并发测试工具
 */
public class ConcurrentTest {
    // 开始闸门
    private CountDownLatch startSignal = null;
    // 结束闸门
    private CountDownLatch downSignal = null;
    // 保存每个线程的任务Task完成时间
    private CopyOnWriteArrayList<Long> list = new CopyOnWriteArrayList<>();
    private ConcurrentTask[] task = null;

    public ConcurrentTest(ConcurrentTask... task) {
        this.task = task;
        if(task == null){
            System.out.println("task can not null");
            System.exit(1);
        }
        downSignal = new CountDownLatch(task.length);

        start();
    }

    private void start() {
        // 创建所有线程并在闸门处等待
        createThread();
        // 打开闸门
        startSignal.countDown();
        try {
            // 等待所有task执行完毕
            downSignal.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // 计算平均响应时间
        getExeTime();
    }

    /**
     * 初始化所有线程并在闸门处等待
     */
    private void createThread() {
        startSignal = new CountDownLatch(1);
        int len = task.length;
        for (int i = 0; i < len; i++) {
            final int j = i;
            new Thread(() -> {
                try {
                    startSignal.await();
                    long start = System.currentTimeMillis();
                    task[j].run();
                    long end = System.currentTimeMillis();
                    list.add(end - start);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                downSignal.countDown();
            }).start();
        }
    }

    /**
     * 计算平均响应时间
     */
    private void getExeTime() {
        int size = list.size();
        List<Long> _list = new ArrayList<Long>(size);
        _list.addAll(list);
        Collections.sort(_list);
        long min = _list.get(0);
        long max = _list.get(size-1);
        long sum = 0L;
        for (Long t : _list) {
            sum += t;
        }
        long avg = sum/size;
        System.out.println("min: " + min);
        System.out.println("max: " + max);
        System.out.println("avg: " + avg);
    }

    public interface ConcurrentTask {
        void run();
    }
}
