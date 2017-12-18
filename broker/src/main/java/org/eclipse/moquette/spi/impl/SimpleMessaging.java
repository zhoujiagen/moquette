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
package org.eclipse.moquette.spi.impl;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import org.HdrHistogram.Histogram;
import org.eclipse.moquette.proto.messages.AbstractMessage;
import org.eclipse.moquette.spi.impl.security.*;
import org.eclipse.moquette.server.ServerChannel;
import org.eclipse.moquette.spi.IMessagesStore;
import org.eclipse.moquette.spi.IMessaging;
import org.eclipse.moquette.spi.ISessionsStore;
import org.eclipse.moquette.spi.impl.events.LostConnectionEvent;
import org.eclipse.moquette.spi.impl.events.MessagingEvent;
import org.eclipse.moquette.spi.impl.events.ProtocolEvent;
import org.eclipse.moquette.spi.impl.events.StopEvent;
import org.eclipse.moquette.spi.impl.subscriptions.SubscriptionsStore;
import org.eclipse.moquette.spi.persistence.MapDBPersistentStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.text.ParseException;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.eclipse.moquette.commons.Constants.PASSWORD_FILE_PROPERTY_NAME;
import static org.eclipse.moquette.commons.Constants.PERSISTENT_STORE_PROPERTY_NAME;
import static org.eclipse.moquette.commons.Constants.ALLOW_ANONYMOUS_PROPERTY_NAME;
import static org.eclipse.moquette.commons.Constants.ACL_FILE_PROPERTY_NAME;

/**
 *
 * Singleton class that orchestrate the execution of the protocol.
 *
 * Uses the LMAX Disruptor to serialize the incoming, requests, because it work in a evented fashion;
 * the requests income from front Netty connectors and are dispatched to the 
 * ProtocolProcessor.
 * <p> MARK 编排协议执行的单例类(是个Disruptor EventHandler). 使用LMAX Disruptor序列化来自前端Netty连接器的请求, 分发到协议处理器ProtocolProcessor.
 * @author andrea
 */
public class SimpleMessaging implements IMessaging, EventHandler<ValueEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(SimpleMessaging.class);
    
    private SubscriptionsStore subscriptions; // MARK 订阅关系存储
    
    private RingBuffer<ValueEvent> m_ringBuffer; // MARK LMAX RingBuffer

    private IMessagesStore m_storageService; // MARK 消息存储
    private ISessionsStore m_sessionsStore; // MARK 会话存储

    private ExecutorService m_executor;
    private Disruptor<ValueEvent> m_disruptor; // MARK LMAX Disruptor

    private static SimpleMessaging INSTANCE;
    
    private final ProtocolProcessor m_processor = new ProtocolProcessor(); // MARK 协议处理器
    private final AnnotationSupport annotationSupport = new AnnotationSupport();
    private boolean benchmarkEnabled = false;
    
    CountDownLatch m_stopLatch;

    Histogram histogram = new Histogram(5);
    
    private SimpleMessaging() {
    }

    public static SimpleMessaging getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new SimpleMessaging();
        }
        return INSTANCE;
    }

    public void init(Properties configProps) {
        subscriptions = new SubscriptionsStore(); // MARK 订阅关系存储
        m_executor = Executors.newFixedThreadPool(1);
        m_disruptor = new Disruptor<>(ValueEvent.EVENT_FACTORY, 1024 * 32, m_executor);
        /*Disruptor<ValueEvent> m_disruptor = new Disruptor<ValueEvent>(ValueEvent.EVENT_FACTORY, 1024 * 32, m_executor,
                ProducerType.MULTI, new BusySpinWaitStrategy());*/
        m_disruptor.handleEventsWith(this); // MARK 设置当前类为Disruptor RingBuffer中事件的事件处理器(EventHandler)
        m_disruptor.start();

        // Get the ring buffer from the Disruptor to be used for publishing.
        m_ringBuffer = m_disruptor.getRingBuffer();

        annotationSupport.processAnnotations(m_processor);
        processInit(configProps);
//        disruptorPublish(new InitEvent(configProps));
    }

    
    private void disruptorPublish(MessagingEvent msgEvent) {
        LOG.debug("disruptorPublish publishing event {}", msgEvent);
        long sequence = m_ringBuffer.next();
        ValueEvent event = m_ringBuffer.get(sequence);

        event.setEvent(msgEvent);
        
        m_ringBuffer.publish(sequence); 
    }
    
    @Override
    public void lostConnection(String clientID) {
        disruptorPublish(new LostConnectionEvent(clientID));
    }

    @Override
    public void handleProtocolMessage(ServerChannel session, AbstractMessage msg) {
        disruptorPublish(new ProtocolEvent(session, msg));
    }

    @Override
    public void stop() {
        m_stopLatch = new CountDownLatch(1);
        disruptorPublish(new StopEvent());
        try {
            //wait the callback notification from the protocol processor thread
            LOG.debug("waiting 10 sec to m_stopLatch");
            boolean elapsed = !m_stopLatch.await(10, TimeUnit.SECONDS);
            LOG.debug("after m_stopLatch");
            m_executor.shutdown();
            m_disruptor.shutdown();
            if (elapsed) {
                LOG.error("Can't stop the server in 10 seconds");
            }
        } catch (InterruptedException ex) {
            LOG.error(null, ex);
        }
    }
    
    @Override
    public void onEvent(ValueEvent t, long l, boolean bln) throws Exception { // MARK EventHandler中方法
        MessagingEvent evt = t.getEvent();
        t.setEvent(null); //free the reference to all Netty stuff
        LOG.info("onEvent processing messaging event from input ringbuffer {}", evt);
        if (evt instanceof StopEvent) {// MARK 停止事件
            processStop();
            return;
        } 
        if (evt instanceof LostConnectionEvent) {// MARK 连接丢失事件
            LostConnectionEvent lostEvt = (LostConnectionEvent) evt;
            m_processor.processConnectionLost(lostEvt);
            return;
        }
        
        if (evt instanceof ProtocolEvent) { // MARK 处理其他协议事件(ProtocolEvent)
            ServerChannel session = ((ProtocolEvent) evt).getSession();
            AbstractMessage message = ((ProtocolEvent) evt).getMessage();
            try {
                long startTime = System.nanoTime();
                annotationSupport.dispatch(session, message); // MARK 使用注解分发协议消息(AbstractMessage)
                if (benchmarkEnabled) {
                    long delay = System.nanoTime() - startTime;
                    histogram.recordValue(delay);
                }
            } catch (Throwable th) {
                LOG.error("Serious error processing the message {} for {}", message, session, th);
            }
        }
    }

    private void processInit(Properties props) {
        benchmarkEnabled = Boolean.parseBoolean(System.getProperty("moquette.processor.benchmark", "false"));

        // MARK 创建消息存储和会话存储 
        //TODO use a property to select the storage path
        MapDBPersistentStore mapStorage = new MapDBPersistentStore(props.getProperty(PERSISTENT_STORE_PROPERTY_NAME, ""));
        m_storageService = mapStorage;
        m_sessionsStore = mapStorage;

        m_storageService.initStore();// MARK 初始化消息存储
        
        //List<Subscription> storedSubscriptions = m_sessionsStore.listAllSubscriptions();
        //subscriptions.init(storedSubscriptions);
        subscriptions.init(m_sessionsStore);// MARK 初始化订阅关系存储

        // MARK 创建验证器和授权器
        String passwdPath = props.getProperty(PASSWORD_FILE_PROPERTY_NAME, "");
        String configPath = System.getProperty("moquette.path", null);
        IAuthenticator authenticator;
        if (passwdPath.isEmpty()) {
            authenticator = new AcceptAllAuthenticator();
        } else {
            authenticator = new FileAuthenticator(configPath, passwdPath);
        }

        String aclFilePath = props.getProperty(ACL_FILE_PROPERTY_NAME, "");
        IAuthorizator authorizator;
        if (aclFilePath != null && !aclFilePath.isEmpty()) {
            authorizator = new DenyAllAuthorizator();
            File aclFile = new File(configPath, aclFilePath);
            try {
                authorizator = ACLFileParser.parse(aclFile);
            } catch (ParseException pex) {
                LOG.error(String.format("Format error in parsing acl file %s", aclFile), pex);
            }
            LOG.info("Using acl file defined at path {}", aclFilePath);
        } else {
            authorizator = new PermitAllAuthorizator();
            LOG.info("Starting without ACL definition");
        }

        // MARK 初始化协议处理器(ProtocolProcessor)
        boolean allowAnonymous = Boolean.parseBoolean(props.getProperty(ALLOW_ANONYMOUS_PROPERTY_NAME, "true"));
        m_processor.init(subscriptions, m_storageService, m_sessionsStore, authenticator, allowAnonymous, authorizator);
    }


    private void processStop() {
        LOG.debug("processStop invoked");
        m_storageService.close();
        LOG.debug("subscription tree {}", subscriptions.dumpTree());
//        m_eventProcessor.halt();
//        m_executor.shutdown();
        
        subscriptions = null;
        m_stopLatch.countDown();

        if (benchmarkEnabled) {
            //log metrics
            histogram.outputPercentileDistribution(System.out, 1000.0);
        }
    }
}
