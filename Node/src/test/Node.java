package test;

import com.hazelcast.config.Config;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.Client;
import com.hazelcast.core.ClientListener;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.util.Scanner;

public class Node {
    private static Logger logger = Logger.getLogger(Node.class);

    private final HazelcastInstance hazelcast;

    private String clientListenerKey;

    public Node()
    {
        XmlConfigBuilder configBuilder = new XmlConfigBuilder();
        Config config = configBuilder.build();
        config.setProperty("hazelcast.logging.type", "log4j");

        hazelcast = Hazelcast.newHazelcastInstance(config);
    }

    public void start()
    {
        clientListenerKey = hazelcast.getClientService().addClientListener(new ClientListener() {
            @Override
            public void clientConnected(Client client) {
                System.out.println("Client connected: "+client.getSocketAddress());
                logger.info("Client connected: "+client.getSocketAddress());
            }

            @Override
            public void clientDisconnected(Client client) {
                System.out.println("Client disconnected: "+client.getSocketAddress());
                logger.info("Client disconnected: "+client.getSocketAddress());
            }
        });
    }

    public void stop()
    {
        hazelcast.getClientService().removeClientListener(clientListenerKey);
        hazelcast.getLifecycleService().shutdown();
    }

    public static void main(String[] args) {
        PropertyConfigurator.configure("log4j.properties");
        
        Node node = new Node();
        node.start();

        System.out.println("Node started");

        System.out.println("Press ENTER to stop node");
        Scanner sc = new Scanner(System.in);
        sc.nextLine();
        System.out.println("Stopping node");

        node.stop();
    }
}
