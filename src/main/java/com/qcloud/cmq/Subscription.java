package com.qcloud.cmq;

import java.util.TreeMap;
import java.util.Vector;

import com.qcloud.cmq.json.JSONArray;
import com.qcloud.cmq.json.JSONObject;

/**
 * TODO subscription class.
 *
 * @author York.
 *         Created 2016年9月27日.
 */
public class Subscription {
	protected String topicName;
	protected String subscriptionName;
	protected CMQClientInterceptor.Chain client;
	/**
	 * TODO construct .
	 *
	 * @param topicName
	 * @param subscriptionName
	 * @param client
	 */
	Subscription(final String topicName , final String subscriptionName, CMQClientInterceptor.Chain client)
	{
		this.topicName = topicName;
		this.subscriptionName = subscriptionName;
		this.client = client;	
	}
	public void ClearFilterTags() throws Exception
    {
    	TreeMap<String, String> param = new TreeMap<String, String>();

		param.put("topicName",this.topicName);
		param.put("subscriptionName", this.subscriptionName);
		String result = this.client.call("ClearSUbscriptionFIlterTags",param);

		CMQTool.checkResult(result);
    
    }
	/**
	 * TODO set subscription attributes.
	 *
	 * @param meta SubscriptionMeata object
	 * @throws Exception
	 */
	public void SetSubscriptionAttributes(SubscriptionMeta meta) throws Exception
	{
		TreeMap<String, String> param = new TreeMap<String, String>();

		param.put("topicName",this.topicName);
		param.put("subscriptionName", this.subscriptionName);
		if( !"".equals(meta.NotifyStrategy)) {
            param.put("notifyStrategy",meta.NotifyStrategy);
        }
		if( !"".equals(meta.NotifyContentFormat)) {
            param.put("notifyContentFormat", meta.NotifyContentFormat);
        }
		if( meta.FilterTag != null )
		{
			int n = 1 ;
			for(String flag : meta.FilterTag)
			{
				param.put("filterTag."+Integer.toString(n), flag);
				++n;
			}
		}
		if( meta.bindingKey != null )
		{
			int n = 1 ;
			for(String flag : meta.bindingKey)
			{
				param.put("bindingKey."+Integer.toString(n), flag);
				++n;
			}
		}
	
		String result = this.client.call("SetSubscriptionAttributes", param);

		CMQTool.checkResult(result);
	}
	
	/**
	 * TODO get subscription attributes.
	 *
	 * @return  subscription meta object
	 * @throws Exception
	 */
	public SubscriptionMeta getSubscriptionAttributes() throws Exception
	{
		TreeMap<String, String> param = new TreeMap<String, String>();

		param.put("topicName",this.topicName);
		param.put("subscriptionName", this.subscriptionName);
		
		String result = this.client.call("GetSubscriptionAttributes", param);
		
		JSONObject jsonObj = new JSONObject(result);
		CMQTool.checkResult(result);

		SubscriptionMeta meta = new SubscriptionMeta();
		meta.FilterTag = new Vector<String>();
        if(jsonObj.has("endpoint")) {
            meta.Endpoint = jsonObj.getString("endpoint");
        }
        if(jsonObj.has("notifyStrategy")) {
            meta.NotifyStrategy = jsonObj.getString("notifyStrategy");
        }
        if(jsonObj.has("notifyContentFormat")) {
            meta.NotifyContentFormat = jsonObj.getString("notifyContentFormat");
        }
        if(jsonObj.has("protocol")) {
            meta.Protocal = jsonObj.getString("protocol");
        }
        if(jsonObj.has("createTime")) {
            meta.CreateTime = jsonObj.getInt("createTime");
        }
        if(jsonObj.has("lastModifyTime")) {
            meta.LastModifyTime = jsonObj.getInt("lastModifyTime");
        }
        if(jsonObj.has("msgCount")) {
            meta.msgCount = jsonObj.getInt("msgCount");
        }
	    if(jsonObj.has("filterTag"))
        {
		    JSONArray jsonArray = jsonObj.getJSONArray("filterTag");
		    if (jsonArray.length() > 0 && meta.FilterTag == null) {
			    meta.FilterTag = new Vector<String>();
		    }
	 	    for(int i=0;i<jsonArray.length();i++)
		    {	
			    meta.FilterTag.add(jsonArray.getString(i));
	    	} 
        }
		if(jsonObj.has("bindingKey"))
        {
		    JSONArray jsonArray = jsonObj.getJSONArray("bindingKey");
			if (jsonArray.length() > 0 && meta.bindingKey == null) {
				meta.bindingKey = new Vector<String>();
			}
	 	    for(int i=0;i<jsonArray.length();i++)
		    {	
			    meta.bindingKey.add(jsonArray.getString(i));
	    	} 
        }
	
		return meta;
	}
	
}
