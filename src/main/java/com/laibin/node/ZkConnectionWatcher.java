package com.laibin.node;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

/**
 * 5种事件类型， 4种连接状态
 */
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ZkConnectionWatcher implements Watcher {

    //计数器对象
    static CountDownLatch countDownLatch = new CountDownLatch(1);
    //连接对象
    static ZooKeeper zooKeeper;

    public void process(WatchedEvent watchedEvent) {
        try {
            if (watchedEvent.getType() == Event.EventType.None) {
                if (watchedEvent.getState() == Event.KeeperState.SyncConnected) {
                    System.out.println("连接创建成功");
                    countDownLatch.countDown();
                } else if (watchedEvent.getState() == Event.KeeperState.Disconnected) {
                    System.out.println("断开连接");
                } else if (watchedEvent.getState() == Event.KeeperState.Expired) {
                    System.out.println("会话超时");
                } else if (watchedEvent.getState() == Event.KeeperState.AuthFailed) {
                    System.out.println("认证失败");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {

        try {
            ZooKeeper zooKeeper = new ZooKeeper("192.168.2.214:2181", 5000, new ZkConnectionWatcher());
            //阻塞线程等待连接的创建
            countDownLatch.await();
            //会话id
            System.out.println(zooKeeper.getSessionId());
            //添加授权用户
            zooKeeper.addAuthInfo("digest", "itcast:123456".getBytes());
            byte[] data = zooKeeper.getData("/node1", false, null);
            System.out.println(new String(data));
            TimeUnit.SECONDS.sleep(5);
            zooKeeper.close();
            System.out.println("结束");
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }
}
