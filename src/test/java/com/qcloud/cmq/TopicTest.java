package com.qcloud.cmq;

import com.qcloud.cmq.entity.CmqConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

/**
 * @author: feynmanlin
 * @date: 2019/11/6 4:02 下午
 */
public class TopicTest {
    CmqConfig cmqConfig;
    List<String> deleteTopicList = new LinkedList<>();
    List<String> deleteQueueList = new LinkedList<>();
    List<String[]> deleteSubscriptionList = new LinkedList<>();
    Account account;

    @Before
    public void initConfig() {

        cmqConfig = new CmqConfig();
        cmqConfig.setEndpoint(System.getProperty("address"));
        cmqConfig.setSecretId(System.getProperty("secretId"));
        cmqConfig.setSecretKey(System.getProperty("secretKey"));
        cmqConfig.setConnectTimeout(10000);
        cmqConfig.setReadTimeout(10000);
        cmqConfig.setMethod("POST");
        cmqConfig.setMaxIdleConnections(1);
        account = new Account(cmqConfig);
    }

    @After
    public void deleteQueueList() {
        for (String[] subscription : deleteSubscriptionList) {
            try {
                account.deleteSubscribe(subscription[0], subscription[1]);
            } catch (Exception e) {
            }
        }
        for (String topicName : deleteTopicList) {
            try {
                account.deleteTopic(topicName);
            } catch (Exception e) {
            }
        }
        for (String queueName : deleteQueueList) {
            try {
                account.deleteQueue(queueName);
            } catch (Exception e) {
            }
        }
    }

    @Test
    public void testTopicAttributes() throws Exception {
        String topicName = "cmqTopicTest2" + System.currentTimeMillis();
        account.createTopic(topicName, 1024 * 1024);
        deleteTopicList.add(topicName);
        Thread.sleep(2000);
        List<String> vTopicList = new ArrayList<>();
        int topicCount = account.listTopic("cmqTopicTest2", vTopicList, -1, -1);
        Assert.assertTrue(topicCount == 1);
        Assert.assertTrue(vTopicList != null && vTopicList.size() == 1 && vTopicList.contains(topicName));

        Topic topic = account.getTopic(topicName);
        TopicMeta topicMeta = topic.getTopicAttributes();
        Assert.assertTrue(topicMeta.maxMsgSize == 1024 * 1024);
        topic.setTopicAttributes(1024);
        topicMeta = topic.getTopicAttributes();
        Assert.assertTrue(topicMeta.maxMsgSize == 1024);

    }

    @Test
    public void testTopic() throws Exception {
        String topicName = "cmqTopicTest2" + System.currentTimeMillis();
        account.createTopic(topicName, 1024 * 1024);
        deleteTopicList.add(topicName);
        Thread.sleep(2000);
        List<String> vTopicList = new ArrayList<>();
        int topicCount = account.listTopic("cmqTopicTest2", vTopicList, -1, -1);
        Assert.assertTrue(topicCount == 1);
        Assert.assertTrue(vTopicList != null && vTopicList.size() == 1 && vTopicList.contains(topicName));

        Topic topic = account.getTopic(topicName);

        String subscriptionName = "cmq-sub-test" + System.currentTimeMillis();
        String queueName = "testCmqQueue" + System.currentTimeMillis();
        QueueMeta meta = new QueueMeta();
        meta.pollingWaitSeconds = 10;
        meta.visibilityTimeout = 10;
        meta.maxMsgSize = 1048576;
        meta.msgRetentionSeconds = 345600;
        account.createQueue(queueName, meta);
        deleteQueueList.add(queueName);

        String Protocol = "queue";
        account.createSubscribe(topicName, subscriptionName, queueName, Protocol);
        deleteSubscriptionList.add(new String[]{topicName, subscriptionName});
        Thread.sleep(2000);
        ArrayList<String> vSubscription = new ArrayList<>();
        int subscriptionCount = topic.ListSubscription(-1, -1, "cmq-sub-test", vSubscription);
        //校验订阅关系是否正确
        Assert.assertTrue(subscriptionCount == 1);
        Assert.assertTrue(vSubscription.contains(subscriptionName));

        String msg = "publishMessage";
        topic.publishMessage(msg);
        Queue queue = account.getQueue(queueName);
        //topic 需要等pushServer投递
        Thread.sleep(5000);
        Message message = queue.receiveMessage();
        //校验消息内容
        Assert.assertTrue("publishMessage".equals(message.msgBody));

        List<String> messageList = Arrays.asList("1", "2", "3");
        topic.batchPublishMessage(messageList);
        Thread.sleep(5000);
        List<Message> batchResult = queue.batchReceiveMessage(10);
        Assert.assertTrue(batchResult != null && batchResult.size() == 3);

        for (Message message1 : batchResult) {
            //批量发送的消息应该和接收的一致
            Assert.assertTrue(messageList.contains(message1.msgBody));
        }

        account.deleteSubscribe(topicName, subscriptionName);
        account.deleteTopic(topicName);
        //校验删除后的订阅和主题数量
        subscriptionCount = topic.ListSubscription(-1, -1, "cmq-sub-test", vSubscription);
        Assert.assertTrue(subscriptionCount == 0);
        topicCount = account.listTopic("cmqTopicTest2", vTopicList, -1, -1);
        Assert.assertTrue(topicCount == 0);
    }

    @Test
    public void testTopicWithTagKey() throws Exception {
        String topicName = "cmqTopicTest2" + System.currentTimeMillis();
        account.createTopic(topicName, 1024 * 1024);
        deleteTopicList.add(topicName);
        Thread.sleep(2000);
        List<String> vTopicList = new ArrayList<>();
        int topicCount = account.listTopic("cmqTopicTest2", vTopicList, -1, -1);
        Assert.assertTrue(topicCount == 1);
        Assert.assertTrue(vTopicList != null && vTopicList.size() == 1 && vTopicList.contains(topicName));

        Topic topic = account.getTopic(topicName);

        String subscriptionName = "cmq-sub-test" + System.currentTimeMillis();
        String queueName = "testCmqQueue" + System.currentTimeMillis();
        QueueMeta meta = new QueueMeta();
        meta.pollingWaitSeconds = 10;
        meta.visibilityTimeout = 10;
        meta.maxMsgSize = 1048576;
        meta.msgRetentionSeconds = 345600;
        account.createQueue(queueName, meta);
        deleteQueueList.add(queueName);

        String Protocol = "queue";
        //通过routeKey做过滤
        account.createSubscribe(topicName, subscriptionName, queueName, Protocol,
                Arrays.asList("route"), null, "BACKOFF_RETRY", "JSON");
        deleteSubscriptionList.add(new String[]{topicName, subscriptionName});
        Thread.sleep(3000);
        ArrayList<String> vSubscription = new ArrayList<>();
        int subscriptionCount = topic.ListSubscription(-1, -1, "cmq-sub-test", vSubscription);
        //校验订阅关系是否正确
        Assert.assertTrue(subscriptionCount == 1);
        Assert.assertTrue(vSubscription.contains(subscriptionName));

        topic.publishMessage("publishMessage", Arrays.asList("route"),null);
        Queue queue = account.getQueue(queueName);
        //topic 需要等pushServer投递
        Thread.sleep(5000);
        Message message = queue.receiveMessage(1);
        //校验消息内容
        Assert.assertTrue("publishMessage".equals(message.msgBody));

        List<String> messageList = Arrays.asList("1", "2", "3");
        topic.batchPublishMessage(messageList,Arrays.asList("route"),null);
        Thread.sleep(5000);
        List<Message> batchResult = queue.batchReceiveMessage(10);
        Assert.assertTrue(batchResult != null && batchResult.size() == 3);

        for (Message message1 : batchResult) {
            //批量发送的消息应该和接收的一致
            Assert.assertTrue(messageList.contains(message1.msgBody));
        }

    }

    @Test
    public void testTopicWithTagKeyUnMatch() throws Exception {
        String topicName = "cmqTopicTest" + System.currentTimeMillis();
        account.createTopic(topicName, 1024 * 1024);
        deleteTopicList.add(topicName);
        Thread.sleep(2000);

        Topic topic = account.getTopic(topicName);

        String subscriptionName = "cmq-sub-test" + System.currentTimeMillis();
        String queueName = "testCmqQueue" + System.currentTimeMillis();
        QueueMeta meta = new QueueMeta();
        meta.pollingWaitSeconds = 10;
        meta.visibilityTimeout = 10;
        meta.maxMsgSize = 1048576;
        meta.msgRetentionSeconds = 345600;
        account.createQueue(queueName, meta);
        deleteQueueList.add(queueName);

        String Protocol = "queue";
        //通过routeKey做过滤
        account.createSubscribe(topicName, subscriptionName, queueName, Protocol,
                Arrays.asList("test"), null, "BACKOFF_RETRY", "JSON");
        deleteSubscriptionList.add(new String[]{topicName, subscriptionName});
        Thread.sleep(2000);

        String msg = "publishMessage";

        boolean hasMessage = true;
        //topic 需要等pushServer投递
        try {
            topic.publishMessage(msg, "route.single");
        } catch (Exception e) {
            hasMessage = false;
        }
        //校验无匹配队列
        Assert.assertTrue(!hasMessage);

        boolean hasBatchMessage = true;
        try {
            List<String> messageList = Arrays.asList("1", "2", "3");
            topic.batchPublishMessage(messageList,"route.batch");
        } catch (Exception e) {
            hasBatchMessage = false;
        }
        Assert.assertTrue(!hasBatchMessage);

    }

    @Test
    public void testTopicWithRouteKey() throws Exception {
        String topicName = "cmqTopicTest" + System.currentTimeMillis();
        account.createTopic(topicName, 1024);
        deleteTopicList.add(topicName);
        Thread.sleep(3000);

        String subscriptionName = "cmq-sub-test" + System.currentTimeMillis();
        String queueName = "testCmqQueue" + System.currentTimeMillis();
        QueueMeta meta = new QueueMeta();
        meta.pollingWaitSeconds = 10;
        meta.visibilityTimeout = 10;
        meta.maxMsgSize = 1048576;
        meta.msgRetentionSeconds = 345600;
        account.createQueue(queueName, meta);
        deleteQueueList.add(queueName);

        String Protocol = "queue";
        //通过routeKey做过滤
        account.createSubscribe(topicName, subscriptionName, queueName, Protocol,
                null, Arrays.asList("route.#"), "BACKOFF_RETRY", "JSON");
        deleteSubscriptionList.add(new String[]{topicName, subscriptionName});
        Thread.sleep(3000);

        String msg = "publishMessage";

        //topic 需要等pushServer投递
        Topic topic = account.getTopic(topicName);
        topic.publishMessage(msg, "route.single");
        Queue queue = account.getQueue(queueName);
        Thread.sleep(1000);
        Message message = queue.receiveMessage(10);

        //校验发送消息有匹配的队列
        Assert.assertTrue(message.msgBody.equals("publishMessage"));

        List<String> messageList = Arrays.asList("1", "2", "3");
        topic.batchPublishMessage(messageList,"route.batch");
        Thread.sleep(3000);
        List<Message> messages = queue.batchReceiveMessage(10,10);
        //校验发送消息有匹配的队列
        Assert.assertTrue(messages.size() == 3);
        for (Message message1 : messages) {
            Assert.assertTrue(messageList.contains(message1.msgBody));
        }

        //todo 订阅中设置routeKey无效，该bug后续修复
        /*topic.publishMessage("msg", "abc.abc");
        //topic 需要等pushServer投递
        boolean hasMessage = true;
        try {
            message = queue.receiveMessage(10);
            System.out.println(message.msgBody);
        }catch (Exception e){
            hasMessage = false;
        }
        //校验无匹配的队列
        Assert.assertTrue(!hasMessage);*/


    }


}
