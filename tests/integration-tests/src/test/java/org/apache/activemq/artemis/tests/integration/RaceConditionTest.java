package org.apache.activemq.artemis.tests.integration;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.client.*;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.CoreQueueConfiguration;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.junit.Before;
import org.junit.Test;

public class RaceConditionTest extends ActiveMQTestBase {

    private String clientQueue = "QueueA";
    private String serverQueue = "QueueB";

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @Test
    public void raceConditionTest() throws Exception {
        Server clientServer = new Server();
        clientServer.start();

        Client client = new Client();
        client.start();

        assertEquals("pingpong", client.sendPing("ping"));

        clientServer.stop();
        clientServer.start();

        assertEquals("ping2pong", client.sendPing("ping2"));

        client.stop();
    }

    class Client {
        ServerLocator locator;
        ClientSession session1;
        ClientSessionFactory factory1;

        ClientSession session2;
        ClientSessionFactory factory2;
        ClientProducer producer;
        ClientConsumer consumer;

        void start() throws Exception {
            TransportConfiguration transport = new TransportConfiguration(NETTY_CONNECTOR_FACTORY);

            locator = ActiveMQClient.createServerLocatorWithoutHA(transport);
            locator.setRetryInterval(1000);
            locator.setRetryIntervalMultiplier(1.5);
            locator.setMaxRetryInterval(60000);
            locator.setReconnectAttempts(-1);

            factory1 = locator.createSessionFactory();
            session1 = factory1.createSession();

            session1.createTemporaryQueue(clientQueue, RoutingType.MULTICAST, clientQueue);
            producer = session1.createProducer(serverQueue);

            session1.start();

            factory2 = locator.createSessionFactory();
            session2 = factory2.createSession();

            consumer = session2.createConsumer(clientQueue);

            session2.start();
        }

        String sendPing(String msg) throws ActiveMQException {
            ClientMessage messagePing = session1.createMessage(false);
            messagePing.writeBodyBufferString(msg);
            producer.send(messagePing);
            session1.commit();

            ClientMessage receivedMsg = consumer.receive();
            receivedMsg.acknowledge();
            int bodySize = receivedMsg.getBodySize();
            String receivedString = receivedMsg.getBodyBuffer().readBytes(bodySize).readString();
            System.out.println("Received from server: " + receivedString);
            return receivedString;
        }

        public void stop() {
            try {
                factory1.close();
                factory2.close();

                locator.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    class Server {
        ServerLocator locator;
        ClientSession session;
        ClientSessionFactory factory;
        ClientProducer producer;
        ClientConsumer consumer;
        ActiveMQServer server;

        public void start() throws Exception {
            Configuration config = createDefaultNettyConfig();
            CoreQueueConfiguration queueConfig = new CoreQueueConfiguration();
            queueConfig.setAddress(serverQueue);
            queueConfig.setName(serverQueue);
            queueConfig.setDurable(false);
            config.addQueueConfiguration(queueConfig);
            server = createServer(false, config);
            server.start();

            TransportConfiguration transport = new TransportConfiguration(NETTY_CONNECTOR_FACTORY);

            locator = ActiveMQClient.createServerLocatorWithoutHA(transport);
            factory = locator.createSessionFactory();
            session = factory.createSession();

            producer = session.createProducer(clientQueue);
            consumer = session.createConsumer(serverQueue);

            consumer.setMessageHandler(message -> {
                int bodySize = message.getBodySize();
                String received = message.getBodyBuffer().readBytes(bodySize).readString();
                System.out.println("Received from client: " + received);

                ClientMessage messagePong = session.createMessage(false);
                messagePong.writeBodyBufferString(received + "pong");

                try {
                    producer.send(messagePong);
                    message.acknowledge();
                } catch (ActiveMQException e) {
                    e.printStackTrace();
                }
            });

            session.start();
        }

        public void stop() {
            try {
                factory.close();
                locator.close();
                server.stop();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
