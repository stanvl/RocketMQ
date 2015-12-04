package com.alibaba.rocketmq.client.hook;

import com.alibaba.rocketmq.client.log.ClientLogger;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.common.message.MessageExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumeMessageClientTraceHook implements ConsumeMessageHook {

    private String name;

    private static final Logger logger = ClientLogger.getLog(LoggerName.RocketmqTracerLoggerName);

    public ConsumeMessageClientTraceHook(String name) {
        this.name = name;
    }

    @Override
    public String hookName() {
        return name;
    }

    @Override
    public void consumeMessageBefore(ConsumeMessageContext context) {
        long timeStamp = System.currentTimeMillis();
        for (MessageExt messageExt : context.getMsgList()) {
            if (!messageExt.isTraceable()) {
                continue;
            }

            logger.info("\"TracerId\": \"{}\", \"MsgId\": \"{}\", \"TimeStamp\": \"{}\", \"Broker\": \"{}\", \"MessageQueue\": \"{}\", " +
                            "\"ConsumerGroup\": \"{}\", \"Client\": \"{}\", \"Topic\": \"{}\", \"Tags\": \"{}\", \"Status\": \"{}\", \"Source\": \"{}\", " +
                            "\"From\":\"{}\", \"To\":\"{}\"",
                    messageExt.getTracerId(),
                    messageExt.getMsgId(),
                    timeStamp,
                    context.getMq().getBrokerName(),
                    context.getMq().getQueueId(),
                    context.getConsumerGroup(),
                    MixAll.localhostName(),
                    messageExt.getTopic(),
                    messageExt.getTags(),
                    "LOAD",
                    "CONSUMER",
                    context.getMq().getBrokerName(),
                    MixAll.localhostName());
        }
    }

    @Override
    public void consumeMessageAfter(ConsumeMessageContext context) {
        long timeStamp = System.currentTimeMillis();
        for (MessageExt messageExt : context.getMsgList()) {
            if(!messageExt.isTraceable()) {
                continue;
            }

            logger.info("\"TracerId\": \"{}\", \"MsgId\": \"{}\", \"TimeStamp\": \"{}\", \"Broker\": \"{}\", \"MessageQueue\": \"{}\", " +
                            "\"ConsumerGroup\": \"{}\", \"Client\": \"{}\", \"Topic\": \"{}\", \"Tags\": \"{}\", \"Status\": \"{}\", \"Source\": \"{}\", " +
                            "\"From\":\"{}\", \"To\":\"{}\"",
                    messageExt.getTracerId(),
                    messageExt.getMsgId(),
                    timeStamp,
                    context.getMq().getBrokerName(),
                    context.getMq().getQueueId(),
                    context.getConsumerGroup(),
                    MixAll.localhostName(),
                    messageExt.getTopic(),
                    messageExt.getTags(),
                    context.getStatus(),
                    "CONSUMER",
                    context.getMq().getBrokerName(),
                    MixAll.localhostName());
        }

    }
}
