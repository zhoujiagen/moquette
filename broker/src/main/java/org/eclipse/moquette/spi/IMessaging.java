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
package org.eclipse.moquette.spi;

import org.eclipse.moquette.proto.messages.AbstractMessage;
import org.eclipse.moquette.server.ServerChannel;

/**
 * Callback interface used to be notified of some events from the input event queue.
 * 
 * It's the abstraction of the messaging stuff attached in after the front protocol
 * parsing stuff.
 * <p> MARK 输入事件队列中的一些事件通知的回调接口. 在前端协议解析后消息处理上的抽象.
 * @author andrea
 */
public interface IMessaging {

    void stop();

    void lostConnection(String clientID);
    
    /**MAKR 处理协议消息, 封装为协议事件(ProtocolEvent), 发送给LMAX Disruptor.*/
    void handleProtocolMessage(ServerChannel session, AbstractMessage msg);
}
