/*
 * Copyright (c) 2012-2014 The original author or authors
 * ------------------------------------------------------
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * and Apache License v2.0 which accompanies this distribution.
 *
 * The Eclipse Public License is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *
 * The Apache License v2.0 is available at
 * http://www.opensource.org/licenses/apache2.0.php
 *
 * You may elect to redistribute this code under either of these licenses.
 */
package org.eclipse.moquette.server.netty;

import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import java.util.HashMap;
import java.util.Map;
import org.eclipse.moquette.spi.IMessaging;
import org.eclipse.moquette.proto.Utils;
import org.eclipse.moquette.proto.messages.AbstractMessage;
import static org.eclipse.moquette.proto.messages.AbstractMessage.*;
import org.eclipse.moquette.proto.messages.PingRespMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MARK MQTT的ChannelInboundHandler实现.
 * @author andrea
 */
@Sharable
public class NettyMQTTHandler extends ChannelInboundHandlerAdapter {
    
    private static final Logger LOG = LoggerFactory.getLogger(NettyMQTTHandler.class);
    private IMessaging m_messaging;
//    private final Map<ChannelHandlerContext, NettyChannel> m_channelMapper = new HashMap<>();
    
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object message) {
        AbstractMessage msg = (AbstractMessage) message;
        LOG.info("Received a message of type {}", Utils.msgType2String(msg.getMessageType()));
        try {
            switch (msg.getMessageType()) {
                case CONNECT:           // 3.01 CONNECT – 连接服务端
                                        // 3.02 CONNACK – 确认连接请求
                case PUBLISH:           // 3.03 PUBLISH – 发布消息
                case PUBACK:            // 3.04 PUBACK –发布确认
                case PUBREC:            // 3.05 PUBREC – 发布收到(QoS 2,第一步)
                case PUBREL:            // 3.06 PUBREL – 发布释放(QoS 2,第二步)
                case PUBCOMP:           // 3.07 PUBCOMP – 发布完成(QoS 2,第三步)
                case SUBSCRIBE:         // 3.08 SUBSCRIBE - 订阅主题
                                        // 3.09 SUBACK – 订阅确认
                case UNSUBSCRIBE:       // 3.10 UNSUBSCRIBE –取消订阅
                                        // 3.11 UNSUBACK – 取消订阅确认
                case DISCONNECT:        // 3.14 DISCONNECT –断开连接

//                    NettyChannel channel;
//                    synchronized(m_channelMapper) {
//                        if (!m_channelMapper.containsKey(ctx)) {
//                            m_channelMapper.put(ctx, new NettyChannel(ctx));
//                        }
//                        channel = m_channelMapper.get(ctx);
//                    }
                    // MARK 转发给IMessaging处理
                    m_messaging.handleProtocolMessage(new NettyChannel(ctx), msg);
                    break;
                case PINGREQ:       // MARK 直接处理 3.12 PINGREQ – 心跳请求,
                                    //              3.13 PINGRESP – 心跳响应
                    PingRespMessage pingResp = new PingRespMessage();
                    ctx.writeAndFlush(pingResp);
                    break;
            }
        } catch (Exception ex) {
            LOG.error("Bad error in processing the message", ex);
        }
    }
    
    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
//        NettyChannel channel = m_channelMapper.get(ctx);
//        String clientID = (String) channel.getAttribute(NettyChannel.ATTR_KEY_CLIENTID);
        String clientID = (String) NettyUtils.getAttribute(ctx, NettyChannel.ATTR_KEY_CLIENTID);
        m_messaging.lostConnection(clientID);
        ctx.close(/*false*/);
//        synchronized(m_channelMapper) {
//            m_channelMapper.remove(ctx);
//        }
    }
    
    public void setMessaging(IMessaging messaging) {
        m_messaging = messaging;
    }
}
