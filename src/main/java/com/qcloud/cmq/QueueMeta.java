package com.qcloud.cmq;

public class QueueMeta {
	
	/** 缺省消息接收长轮询等待时间 */
	public static final int DEFAULT_POLLING_WAIT_SECONDS = 0;
	/** 缺省消息可见性超时 */
	public static final int DEFAULT_VISIBILITY_TIMEOUT = 30;
	/** 缺省消息最大长度，单位字节 */
	public static final int DEFAULT_MAX_MSG_SIZE = 65536;
	/** 缺省消息保留周期，单位秒 */
	public static final int DEFAULT_MSG_RETENTION_SECONDS = 345600;
	
	/** 最大堆积消息数 */
	public int maxMsgHeapNum = -1;
	/** 消息接收长轮询等待时间 */
	public int pollingWaitSeconds = DEFAULT_POLLING_WAIT_SECONDS;
	/** 消息可见性超时 */
	public int visibilityTimeout = DEFAULT_VISIBILITY_TIMEOUT;
	/** 消息最大长度 */
	public int maxMsgSize = DEFAULT_MAX_MSG_SIZE;
	/** 消息保留周期 */
	public int msgRetentionSeconds = DEFAULT_MSG_RETENTION_SECONDS;
	/** 队列创建时间 */
	public int createTime = -1;
	/** 队列属性最后修改时间 */
	public int lastModifyTime = -1;
	/** 队列处于Active状态的消息总数 */
	public int activeMsgNum = -1;
	/** 队列处于Inactive状态的消息总数 */
	public int inactiveMsgNum = -1;

	/** 已删除的消息，但还在回溯保留时间内的消息数量 */
	public int rewindmsgNum;
	/** 消息最小未消费时间 */
	public int minMsgTime;
	/** 延时消息数量 */
	public int delayMsgNum;
	
}
