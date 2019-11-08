package com.qcloud.cmq;

import com.qcloud.cmq.entity.CmqConfig;
import com.qcloud.cmq.entity.CmqResponse;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * @author: feynmanlin
 * @date: 2019/11/6 10:36 上午
 */

public class QueueTest {
    CmqConfig cmqConfig;
    List<String> deleteQueueList = new LinkedList<>();
    Account account;

    @Before
    public void initConfig() {

        cmqConfig = new CmqConfig();
        cmqConfig.setEndpoint("");
        cmqConfig.setSecretId("");
        cmqConfig.setSecretKey("");
        cmqConfig.setConnectTimeout(10000);
        cmqConfig.setReadTimeout(10000);
        cmqConfig.setReceiveTimeout(15000);
        cmqConfig.setMaxIdleConnections(1);
        account = new Account(cmqConfig);
    }

    @After
    public void deleteQueueList() {
        for (String queueName : deleteQueueList) {
            try {
                account.deleteQueue(queueName);
            } catch (Exception e) {
            }
        }
    }

    @Test
    public void testQueue() throws Exception {

        QueueMeta meta = new QueueMeta();
        meta.pollingWaitSeconds = 10;
        meta.visibilityTimeout = 10;
        meta.maxMsgSize = 1048576;
        meta.msgRetentionSeconds = 345600;
        //创建2个队列，并验证查询结果是否正确
        String queueName1 = "testCmqQueue2" + System.currentTimeMillis();
        deleteQueueList.add(queueName1);
        account.createQueue(queueName1, meta);

        String queueName2 = "testCmqQueue2" + System.currentTimeMillis();
        deleteQueueList.add(queueName2);
        account.createQueue(queueName2, meta);

        ArrayList<String> vtQueue = new ArrayList<String>();
        int totalCount = account.listQueue("testCmqQueue2", -1, -1, vtQueue);

        Assert.assertEquals(2, totalCount);
        Assert.assertTrue(vtQueue.contains(queueName1));
        Assert.assertTrue(vtQueue.contains(queueName2));
        //删除队列，并验证查询结果是否正确
        account.deleteQueue(queueName1);
        deleteQueueList.remove(queueName1);
        account.deleteQueue(queueName2);
        deleteQueueList.remove(queueName2);

        totalCount = account.listQueue("testCmqQueue2", -1, -1, vtQueue);
        Assert.assertEquals(0, totalCount);
    }

    @Test
    public void testQueueAttributes() throws Exception {
        //设置参数并创建队列，并验证参数是否正确
        QueueMeta meta = new QueueMeta();
        meta.pollingWaitSeconds = 10;
        meta.visibilityTimeout = 10;
        meta.maxMsgSize = 1048576;
        meta.msgRetentionSeconds = 345600;
        String queueName1 = "testCmqQueue" + System.currentTimeMillis();
        deleteQueueList.add(queueName1);
        account.createQueue(queueName1, meta);
        Queue queue = account.getQueue(queueName1);
        QueueMeta meta2 = queue.getQueueAttributes();
        Assert.assertEquals(10, meta2.pollingWaitSeconds);
        Assert.assertEquals(10, meta2.visibilityTimeout);
        Assert.assertEquals(1048576, meta2.maxMsgSize);
        Assert.assertEquals(345600, meta2.msgRetentionSeconds);
        //验证修改参数是否正确
        meta.pollingWaitSeconds = 20;
        queue.setQueueAttributes(meta);
        meta2 = queue.getQueueAttributes();
        Assert.assertEquals(20, meta2.pollingWaitSeconds);
        Assert.assertEquals(10, meta2.visibilityTimeout);
        Assert.assertEquals(1048576, meta2.maxMsgSize);
        Assert.assertEquals(345600, meta2.msgRetentionSeconds);
    }

    @Test
    public void testSingleMessage() throws Exception {
        QueueMeta meta = new QueueMeta();
        meta.pollingWaitSeconds = 10;
        meta.visibilityTimeout = 10;
        meta.maxMsgSize = 1048576;
        meta.msgRetentionSeconds = 345600;
        String queueName1 = "testCmqQueue" + System.currentTimeMillis();
        deleteQueueList.add(queueName1);
        account.createQueue(queueName1, meta);
        LocalDateTime now = LocalDateTime.now();
        Thread.sleep(5000);
        Queue queue = account.getQueue(queueName1);
        //使用老接口，发送并接收消息，验证消息内容一样，消息进队时间晚于发送时间，出队次数为1次等
        String msgId = queue.sendMessage("hello world,this is cmq sdk for java");
        Assert.assertTrue(msgId != null && msgId.trim().length() > 0);
        Thread.sleep(3000);

        Message msg = queue.receiveMessage();

        Assert.assertEquals("hello world,this is cmq sdk for java", msg.msgBody);
        Assert.assertEquals(1, msg.dequeueCount);

        Instant instant = Instant.ofEpochSecond(msg.enqueueTime);
        ZoneId zone = ZoneId.systemDefault();

        LocalDateTime enqueueTime = LocalDateTime.ofInstant(instant, zone);
        Assert.assertTrue(now.isBefore(enqueueTime));
        Instant dequeueInstant = Instant.ofEpochSecond(msg.firstDequeueTime);
        LocalDateTime dequeueTime = LocalDateTime.ofInstant(dequeueInstant, zone);
        Assert.assertTrue(enqueueTime.isBefore(dequeueTime) || enqueueTime.isEqual(dequeueTime));
        queue.deleteMessage(msg.receiptHandle);


    }

    @Test
    public void testSingleMessageNew() throws Exception {
        QueueMeta meta = new QueueMeta();
        meta.pollingWaitSeconds = 10;
        meta.visibilityTimeout = 10;
        meta.maxMsgSize = 1048576;
        meta.msgRetentionSeconds = 345600;
        String queueName1 = "testCmqQueue" + System.currentTimeMillis();
        deleteQueueList.add(queueName1);
        //使用新接口，发送并接收消息，验证消息内容一样，消息进队时间晚于发送时间，出队次数为1次等
        account.createQueue(queueName1, meta);
        LocalDateTime now = LocalDateTime.now();
        Thread.sleep(3000);
        Queue queue = account.getQueue(queueName1);
        Thread.sleep(3000);
        CmqResponse cmqResponse = queue.send("hello world,this is cmq sdk for java");
        Assert.assertTrue(cmqResponse != null && cmqResponse.getMsgId().trim().length() > 0);
        Assert.assertTrue(cmqResponse != null && cmqResponse.getRequestId().trim().length() > 0);

        Message msg = queue.receiveMessage();

        Assert.assertEquals("hello world,this is cmq sdk for java", msg.msgBody);
        Assert.assertEquals(1, msg.dequeueCount);

        Instant instant = Instant.ofEpochSecond(msg.enqueueTime);
        ZoneId zone = ZoneId.systemDefault();
        LocalDateTime enqueueTime = LocalDateTime.ofInstant(instant, zone);
        Assert.assertTrue(now.isBefore(enqueueTime));

        Instant dequeueInstant = Instant.ofEpochSecond(msg.firstDequeueTime);
        LocalDateTime dequeueTime = LocalDateTime.ofInstant(dequeueInstant, zone);
        Assert.assertTrue(enqueueTime.isBefore(dequeueTime) || enqueueTime.isEqual(dequeueTime));

        queue.deleteMessage(msg.receiptHandle);
    }

    @Test
    public void testSingleMessageReceive() throws Exception {
        QueueMeta meta = new QueueMeta();
        meta.pollingWaitSeconds = 10;
        meta.visibilityTimeout = 10;
        meta.maxMsgSize = 1048576;
        meta.msgRetentionSeconds = 345600;
        String queueName1 = "testCmqQueue" + System.currentTimeMillis();
        deleteQueueList.add(queueName1);
        //使用新接口，发送并使用老接口接收消息
        account.createQueue(queueName1, meta);
        LocalDateTime now = LocalDateTime.now();
        Thread.sleep(3000);
        Queue queue = account.getQueue(queueName1);
        Thread.sleep(5000);
        CmqResponse cmqResponse = queue.send("hello world,this is cmq sdk for java");
        Assert.assertTrue(cmqResponse != null && cmqResponse.getMsgId().trim().length() > 0);
        Assert.assertTrue(cmqResponse != null && cmqResponse.getRequestId().trim().length() > 0);

        Message msg = queue.receiveMessage(10);

        Assert.assertEquals("hello world,this is cmq sdk for java", msg.msgBody);
        Assert.assertEquals(1, msg.dequeueCount);

        Instant instant = Instant.ofEpochSecond(msg.enqueueTime);
        ZoneId zone = ZoneId.systemDefault();
        LocalDateTime enqueueTime = LocalDateTime.ofInstant(instant, zone);
        Assert.assertTrue(now.isBefore(enqueueTime));

        Instant dequeueInstant = Instant.ofEpochSecond(msg.firstDequeueTime);
        LocalDateTime dequeueTime = LocalDateTime.ofInstant(dequeueInstant, zone);
        Assert.assertTrue(enqueueTime.isBefore(dequeueTime) || enqueueTime.isEqual(dequeueTime));

        queue.deleteMessage(msg.receiptHandle);
    }

    @Test
    public void testDelayMessage() throws Exception {
        //验证老接口延迟消息是否正常
        QueueMeta meta = new QueueMeta();
        meta.pollingWaitSeconds = 10;
        meta.visibilityTimeout = 10;
        meta.maxMsgSize = 1048576;
        meta.msgRetentionSeconds = 345600;
        String queueName1 = "testCmqQueue" + System.currentTimeMillis();
        deleteQueueList.add(queueName1);
        account.createQueue(queueName1, meta);
        Thread.sleep(3000);
        Queue queue = account.getQueue(queueName1);
        Thread.sleep(3000);
        //12秒延迟消息，长轮询100ms+读等待时间10s，肯定不可见
        queue.sendMessage("delayMessage", 12);
        boolean canSee = false;
        Message msg = null;
        try {
            msg = queue.receiveMessage();
        } catch (Exception e) {
            canSee = true;
        }
        Assert.assertTrue(canSee);
        Thread.sleep(12000);
        msg = queue.receiveMessage();
        Assert.assertTrue(msg != null);
        Assert.assertTrue(msg.requestId != null);
        Assert.assertTrue(msg.msgId != null);
        queue.deleteMessage(msg.receiptHandle);
    }

    @Test
    public void testBatchMessage() throws Exception {
        //使用老接口，批量发送并接收消息，验证消息内容一样，消息进队时间晚于发送时间，出队次数为1次等
        QueueMeta meta = new QueueMeta();
        meta.pollingWaitSeconds = 10;
        meta.visibilityTimeout = 10;
        meta.maxMsgSize = 1048576;
        meta.msgRetentionSeconds = 345600;
        String queueName1 = "testCmqQueue" + System.currentTimeMillis();
        deleteQueueList.add(queueName1);
        account.createQueue(queueName1, meta);
        LocalDateTime now = LocalDateTime.now();
        Thread.sleep(3000);
        Queue queue = account.getQueue(queueName1);
        Thread.sleep(3000);
        ArrayList<String> vtMsgBody = new ArrayList<String>();
        String msgBody = "hello world,this is cmq sdk for java 1";
        vtMsgBody.add(msgBody);
        msgBody = "hello world,this is cmq sdk for java 2";
        vtMsgBody.add(msgBody);
        msgBody = "hello world,this is cmq sdk for java 3";
        vtMsgBody.add(msgBody);
        List<String> vtMsgId = queue.batchSendMessage(vtMsgBody);
        Assert.assertTrue(vtMsgId != null && vtMsgId.size() == 3);
        Thread.sleep(2000);
        //保存服务器返回的消息句柄，用于删除消息
        ArrayList<String> vtReceiptHandle = new ArrayList<>();
        List<Message> msgList = queue.batchReceiveMessage(10);
        Assert.assertTrue(msgList != null && msgList.size() == 3);
        for (int i = 0; i < msgList.size(); i++) {
            Message msg1 = msgList.get(i);
            Instant instant = Instant.ofEpochSecond(msg1.enqueueTime);
            ZoneId zone = ZoneId.systemDefault();
            LocalDateTime enqueueTime = LocalDateTime.ofInstant(instant, zone);
            Assert.assertTrue(now.isBefore(enqueueTime));

            Instant dequeueInstant = Instant.ofEpochSecond(msg1.firstDequeueTime);
            LocalDateTime dequeueTime = LocalDateTime.ofInstant(dequeueInstant, zone);
            Assert.assertTrue(enqueueTime.isBefore(dequeueTime) || enqueueTime.isEqual(dequeueTime));

            Assert.assertEquals(1, msg1.dequeueCount);

            vtReceiptHandle.add(msg1.receiptHandle);
        }

        queue.batchDeleteMessage(vtReceiptHandle);
    }

    @Test
    public void testBatchMessageNew() throws Exception {
        //使用新接口，发送并接收消息，验证消息内容一样，消息进队时间晚于发送时间，出队次数为1次等
        QueueMeta meta = new QueueMeta();
        meta.pollingWaitSeconds = 10;
        meta.visibilityTimeout = 10;
        meta.maxMsgSize = 1048576;
        meta.msgRetentionSeconds = 345600;
        String queueName1 = "testCmqQueue" + System.currentTimeMillis();
        deleteQueueList.add(queueName1);
        account.createQueue(queueName1, meta);
        LocalDateTime now = LocalDateTime.now();
        Thread.sleep(3000);
        Queue queue = account.getQueue(queueName1);
        Thread.sleep(5000);
        ArrayList<String> vtMsgBody = new ArrayList<String>();
        String msgBody = "hello world,this is cmq sdk for java 1";
        vtMsgBody.add(msgBody);
        msgBody = "hello world,this is cmq sdk for java 2";
        vtMsgBody.add(msgBody);
        msgBody = "hello world,this is cmq sdk for java 3";
        vtMsgBody.add(msgBody);
        List<CmqResponse> cmqResponses = queue.batchSend(vtMsgBody);
        Assert.assertTrue(cmqResponses != null && cmqResponses.size() == 3);
        Thread.sleep(2000);
        //保存服务器返回的消息句柄，用于删除消息
        ArrayList<String> vtReceiptHandle = new ArrayList<>();
        List<Message> msgList = queue.batchReceiveMessage(10);
        Assert.assertTrue(msgList != null && msgList.size() == 3);
        for (int i = 0; i < msgList.size(); i++) {
            Message msg1 = msgList.get(i);
            Instant instant = Instant.ofEpochSecond(msg1.enqueueTime);
            ZoneId zone = ZoneId.systemDefault();
            LocalDateTime enqueueTime = LocalDateTime.ofInstant(instant, zone);
            Assert.assertTrue(now.isBefore(enqueueTime));

            Instant dequeueInstant = Instant.ofEpochSecond(msg1.firstDequeueTime);
            LocalDateTime dequeueTime = LocalDateTime.ofInstant(dequeueInstant, zone);
            Assert.assertTrue(enqueueTime.isBefore(dequeueTime) || enqueueTime.isEqual(dequeueTime));

            Assert.assertEquals(1, msg1.dequeueCount);

            vtReceiptHandle.add(msg1.receiptHandle);
        }

        queue.batchDeleteMessage(vtReceiptHandle);
    }

    @Test
    public void testBatchMessageReceive() throws Exception {
        //使用新接口，发送并使用老接口接收消息
        QueueMeta meta = new QueueMeta();
        meta.pollingWaitSeconds = 10;
        meta.visibilityTimeout = 10;
        meta.maxMsgSize = 1048576;
        meta.msgRetentionSeconds = 345600;
        String queueName1 = "testCmqQueue" + System.currentTimeMillis();
        deleteQueueList.add(queueName1);
        account.createQueue(queueName1, meta);
        LocalDateTime now = LocalDateTime.now();
        Thread.sleep(3000);
        Queue queue = account.getQueue(queueName1);
        Thread.sleep(3000);
        ArrayList<String> vtMsgBody = new ArrayList<String>();
        String msgBody = "hello world,this is cmq sdk for java 1";
        vtMsgBody.add(msgBody);
        msgBody = "hello world,this is cmq sdk for java 2";
        vtMsgBody.add(msgBody);
        msgBody = "hello world,this is cmq sdk for java 3";
        vtMsgBody.add(msgBody);
        List<CmqResponse> cmqResponses = queue.batchSend(vtMsgBody);
        Assert.assertTrue(cmqResponses != null && cmqResponses.size() == 3);
        Thread.sleep(2000);
        //保存服务器返回的消息句柄，用于删除消息
        ArrayList<String> vtReceiptHandle = new ArrayList<>();
        List<Message> msgList = queue.batchReceiveMessage(10, 10);
        Assert.assertTrue(msgList != null && msgList.size() == 3);
        for (int i = 0; i < msgList.size(); i++) {
            Message msg1 = msgList.get(i);
            Instant instant = Instant.ofEpochSecond(msg1.enqueueTime);
            ZoneId zone = ZoneId.systemDefault();
            LocalDateTime enqueueTime = LocalDateTime.ofInstant(instant, zone);
            Assert.assertTrue(now.isBefore(enqueueTime));

            Instant dequeueInstant = Instant.ofEpochSecond(msg1.firstDequeueTime);
            LocalDateTime dequeueTime = LocalDateTime.ofInstant(dequeueInstant, zone);
            Assert.assertTrue(enqueueTime.isBefore(dequeueTime) || enqueueTime.isEqual(dequeueTime));

            Assert.assertEquals(1, msg1.dequeueCount);

            vtReceiptHandle.add(msg1.receiptHandle);
        }

        queue.batchDeleteMessage(vtReceiptHandle);
    }

    @Test
    public void testTimeout() throws Exception {
        QueueMeta meta = new QueueMeta();
        meta.pollingWaitSeconds = 20;
        meta.visibilityTimeout = 10;
        meta.maxMsgSize = 1048576;
        meta.msgRetentionSeconds = 345600;
        String queueName1 = "testCmqQueue" + System.currentTimeMillis();
        deleteQueueList.add(queueName1);
        account.createQueue(queueName1, meta);
        Thread.sleep(5000);
        Queue queue = account.getQueue(queueName1);
        long start = System.currentTimeMillis();
        Message message = null;
        try {
            //等待receiveTimeout的时间15秒，队列长轮询时间20秒，因此超时时间为15秒
            message = queue.receiveMessage();
        } catch (Exception e) {

        }
        long lastTime = System.currentTimeMillis() - start;
        Assert.assertTrue(message == null);
        Assert.assertTrue("last for " + lastTime + " pollingTime:" + 15000, lastTime > 14000 );
        Assert.assertTrue("last for " + lastTime + " pollingTime:" + 15000, 16000  >= lastTime);
        start = System.currentTimeMillis();
        try {
            //客户端控制长轮询3秒，超时时间 = 3秒
            queue.receiveMessage(3);
        } catch (Exception e) {

        }
        lastTime = System.currentTimeMillis() - start;
        Assert.assertTrue("last for " + lastTime, 4000 >= lastTime);
        Assert.assertTrue("last for " + lastTime, lastTime >= 2500);
        start = System.currentTimeMillis();
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.sleep(2000);
                    queue.send("msg");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }).start();
        message = queue.receiveMessage(5);
        lastTime = System.currentTimeMillis() - start;
        Assert.assertTrue("msg".equals(message.msgBody));
        Assert.assertTrue("last for " + lastTime,lastTime >= 2000);
        Assert.assertTrue("last for " + lastTime,lastTime <= 3000);

    }

}
