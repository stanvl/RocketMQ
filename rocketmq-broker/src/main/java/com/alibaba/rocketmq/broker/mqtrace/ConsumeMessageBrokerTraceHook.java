package com.alibaba.rocketmq.broker.mqtrace;

import ch.qos.logback.core.joran.spi.JoranException;

import com.alibaba.rocketmq.broker.BrokerLogFactory;
import com.alibaba.rocketmq.common.constant.LoggerName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumeMessageBrokerTraceHook implements ConsumeMessageHook {

    private String name;

    private static final Logger log = LoggerFactory.getLogger(LoggerName.RocketmqTracerLoggerName);

    public ConsumeMessageBrokerTraceHook(String name) throws JoranException {
        this.name = name;
        //log = BrokerLogFactory.getLogger(LoggerName.RocketmqTracerLoggerName);
    }

    @Override
    public String hookName() {
        return name;
    }

    @Override
    public void consumeMessageBefore(ConsumeMessageContext context) {
            long timeStamp = System.currentTimeMillis();
            for (String msgId : context.getMessageIds().keySet()) {
                log.info("\"MsgId\": \"{}\", \"TimeStamp\": \"{}\", \"Broker\": \"{}\", \"MessageQueue\": \"{}\", " +
                            "\"ConsumerGroup\": \"{}\", \"Client\": \"{}\", \"Status\": \"{}\", \"Source\": \"{}\", " +
                            "\"From\":\"{}\", \"To\":\"{}\"",
                        msgId,
                        timeStamp,
                        context.getStoreHost(),
                        context.getQueueId(),
                        context.getConsumerGroup(),
                        context.getClientHost(),
                        "CLIENT REQUEST",
                        "BROKER",
                        context.getStoreHost(),
                        context.getClientHost());
            }
    }

    @Override
    public void consumeMessageAfter(ConsumeMessageContext context) {
            long timeStamp = System.currentTimeMillis();
            for (String msgId : context.getMessageIds().keySet()) {
                log.info("\"MsgId\": \"{}\", \"TimeStamp\": \"{}\", \"ConsumerGroup\": \"{}\", \"Client\": \"{}\", " +
                                "\"Broker\": \"{}\", \"MessageQueue\": \"{}\", \"Status\": \"{}\", \"Source\": \"{}\", " +
                                "\"From\":\"{}\", \"To\":\"{}\"",
                        msgId,
                        timeStamp,
                        context.getConsumerGroup(),
                        context.getClientHost(),
                        context.getStoreHost(),
                        context.getQueueId(),
                        context.getStatus(),
                        "BROKER",
                        context.getStoreHost(),
                        context.getClientHost());
            }
    }

}
