package cn.qm.cn.qm.test;/*
Author:an
date:create in 2019/1/14 0014 20:52
describe:
*/




import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class ThreadExcutor {

    //创建
    private volatile boolean RUNNING = true;

    //所有任务都放队列中，让工作线程来消费
    private static BlockingQueue<Runnable> queue = null;

    private final HashSet<Worker> workers = new HashSet<Worker>();

    private final List<Thread> threadList = new ArrayList<Thread>();

    //工作线程数
    int poolSize = 0;
    //核心线程数（创建了多少个工作线程）
    int coreSize = 0;

    boolean shutdown = false;

    public ThreadExcutor(int poolSize) {
        this.poolSize = poolSize;
        queue = new LinkedBlockingQueue<Runnable>(poolSize);
    }

    //执行传进来的线程任务
    public void exec(Runnable runnable) {
        if (runnable == null) throw new NullPointerException();
        //当核心线程数小于工作线程数的时候
        if (coreSize < poolSize) {
            addThread(runnable);
        } else {
            //System.out.println("offer" +  runnable.toString() + "   " + queue.size());
            try {
                queue.put(runnable);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public void addThread(Runnable runnable) {
        coreSize++;
        Worker worker = new Worker(runnable);
        workers.add(worker);
        Thread t = new Thread(worker);
        threadList.add(t);
        try {
            t.start();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public void shutdown() {
        RUNNING = false;
        if (!workers.isEmpty()) {
            for (Worker worker : workers) {
                worker.interruptIfIdle();
            }
        }
        shutdown = true;
        Thread.currentThread().interrupt();
    }
    //这里留个位置放内部类Worker
    /**
     * 工作线程
     */
    class  Worker implements Runnable{

        public Worker(Runnable runnable){
            //在queue中插入runnable
            queue.offer(runnable);
        }

        @Override
        public void run() {
            while (true && RUNNING){
                if(shutdown == true){
                    Thread.interrupted();
                }
                Runnable task = null;
                try {
                    //从任务队列中拿取一个任务
                    task = getTask();
                    //执行这个任务
                    task.run();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        public Runnable getTask() throws InterruptedException {
            return queue.take();
        }

        public void interruptIfIdle() {
            for (Thread thread :threadList) {
                System.out.println(thread.getName() + " interrupt");
                thread.interrupt();
            }
        }
    }
}