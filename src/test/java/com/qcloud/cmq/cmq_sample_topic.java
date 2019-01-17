package com.qcloud.cmq;
import java.lang.*;
import java.util.ArrayList;
import java.util.Vector;

public class cmq_sample_topic {


	public static void main(String[] args) {
		// 请在腾讯云官网查看 id ,key endpoint
		String secretId="";
		String secretKey="";
		String endpoint = "";
		String path = "/v2/index.php";
		String method = "POST";

		try
		{
			int batchMessageSize = 5 ;
			// create account;
			Account account = new Account(endpoint,secretId, secretKey);
			// create topic
			System.out.println("init account ");
			String topicName="topic-test";

			account.createTopic(topicName, 1024*1024);
			System.out.println("create topic");
			// get topic meta
			Topic topic = account.getTopic(topicName);
			Thread.sleep(1000);
			TopicMeta topicMeta = new TopicMeta();

			// set  and get topic meta
			topicMeta.maxMsgSize = 32768;
			topic.setTopicAttributes(topicMeta.maxMsgSize);
			topicMeta = topic.getTopicAttributes();
			System.out.println("set and get topic meta  ");

			// create subscription input your endpoint and protocol
			String subscriptionName = "sub-test";
			String Endpoint = "queue-test10";
			String Protocol = "queue";
			account.createSubscribe(topicName,subscriptionName, Endpoint, Protocol);

			System.out.println("create sub ");
			// set subscription meta
			Subscription  sub = account.getSubscription(topicName,subscriptionName);
			SubscriptionMeta  subscriptionMeta = sub.getSubscriptionAttributes();

			System.out.println("set sub meta  ");
			// list subscription
			ArrayList< String> vSubscription = new ArrayList<String>();

			int SubscriptionCount = topic.ListSubscription(-1,-1,"",vSubscription);
			for (String subscription : vSubscription)
			{
				System.out.println("Subscription name :" + subscription);
			}

			System.out.println("list sub ");


			// publish message and batch publish message
			Vector<String> vMsg = new Vector<String>();
			for(int i = 0 ; i< batchMessageSize ; ++ i )
			{
				String msg ="this is a test message publish";
				vMsg.add(msg);
			}

			// publish message without tag
			String msg = "this is a test message";
			topic.publishMessage(msg);
			System.out.println("publish message  ");
			topic.batchPublishMessage(vMsg);

			System.out.println(" batch publish message");
			Vector<String>  vTopicList = new Vector<String>();
			account.listTopic("",vTopicList,-1,-1);
			for( String TopicName : vTopicList)
			{
				System.out.println(TopicName);
			}

			// delete subscription and topic
			account.deleteSubscribe(topicName,subscriptionName);
			System.out.println("delete sub ");
			account.deleteTopic(topicName);
			System.out.println("delete topic  ");
		}
		catch(CMQServerException e1){
			System.out.println("Server Exception, " + e1.toString());
		}
		catch(CMQClientException e2){
			System.out.println("Client Exception, " + e2.toString());
		}
		catch (Exception e) {
			System.out.println("error..." + e.toString());
		}
	}
}
