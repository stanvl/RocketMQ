package com.alibaba.rocketmq.broker.mqtrace;

import ch.qos.logback.core.joran.spi.JoranException;

import com.alibaba.rocketmq.broker.BrokerLogFactory;
import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.common.message.MessageConst;
import com.alibaba.rocketmq.common.message.MessageDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class SendMessageBrokerTraceHook implements SendMessageHook {

    private String name;

    private static final Logger log = LoggerFactory.getLogger(LoggerName.RocketmqTracerLoggerName);

    public SendMessageBrokerTraceHook(String name) throws JoranException {
        this.name = name;
        //log = BrokerLogFactory.getLogger(LoggerName.RocketmqTracerLoggerName);
    }

    @Override
    public String hookName() {
        return name;
    }

    @Override
    public void sendMessageBefore(SendMessageContext context) {
        Map<String, String> properties = MessageDecoder.string2messageProperties(context.getMsgProps());
        if (!properties.containsKey(MessageConst.PROPERTY_MESSAGE_TRACE_ID)) {
            return;
        }

        long timeStamp = System.currentTimeMillis();
        log.info("\"TracerId\": \"{}\", \"TimeStamp\": \"{}\", \"ProducerGroup\": \"{}\", \"BornHost\": \"{}\", \"Topic\": \"{}\", \"Tags\": \"{}\", \"MsgId\": \"{}\", " +
                    "\"Broker\": \"{}\", \"MessageQueue\": \"{}\", \"OffSet\": \"{}\", \"Status\": \"{}\", \"Source\": \"{}\", " +
                    "\"From\":\"{}\", \"To\":\"{}\"",
                properties.get(MessageConst.PROPERTY_MESSAGE_TRACE_ID),
                timeStamp,
                context.getProducerGroup(),
                context.getBornHost(),
                context.getTopic(),
                properties.get(MessageConst.PROPERTY_TAGS),
                null,
                context.getBrokerAddr(),
                context.getQueueId(),
                context.getQueueOffset(),
                "RECEIVED",
                "BROKER",
                context.getBornHost(),
                context.getBrokerAddr());

    }

    @Override
    public void sendMessageAfter(SendMessageContext context) {
        Map<String, String> properties = MessageDecoder.string2messageProperties(context.getMsgProps());
        if (!properties.containsKey(MessageConst.PROPERTY_MESSAGE_TRACE_ID)) {
            return;
        }
        
        long timeStamp = System.currentTimeMillis();
        log.info("\"TracerId\": \"{}\", \"TimeStamp\": \"{}\", \"ProducerGroup\": \"{}\", \"BornHost\": \"{}\", \"Topic\": \"{}\", \"Tags\": \"{}\", \"MsgId\": \"{}\", " +
                        "\"Broker\": \"{}\", \"MessageQueue\": \"{}\", \"OffSet\": \"{}\", \"Status\": \"{}\", \"Source\": \"{}\", " +
                        "\"From\":\"{}\", \"To\":\"{}\"",
                properties.get(MessageConst.PROPERTY_MESSAGE_TRACE_ID),
                timeStamp,
                context.getProducerGroup(),
                context.getBornHost(),
                context.getTopic(),
                properties.get(MessageConst.PROPERTY_TAGS),
                context.getMsgId(),
                context.getBrokerAddr(),
                context.getQueueId(),
                context.getQueueOffset(),
                "STORED",
                "BROKER",
                context.getBornHost(),
                context.getBrokerAddr());

    }
}
