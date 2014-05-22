package test;

import com.hazelcast.config.Config;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.Client;
import com.hazelcast.core.ClientListener;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.util.Collection;
import java.util.Scanner;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

public class Node {
    private static Logger logger = Logger.getLogger(Node.class);
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
        @Override
        public Thread newThread(Runnable runnable) {
            Thread thread = Executors.defaultThreadFactory().newThread(runnable);
            thread.setDaemon(true);
            return thread;        }
    });

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
        scheduler.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                dump();
            }
        }, 2, 2, TimeUnit.SECONDS);

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

    private void dump() {
        Collection<Client> clients = hazelcast.getClientService().getConnectedClients();
        System.out.println("Clients connected: "+clients.size());
        for(Client client : clients)
        {
            System.out.println("- Type: "+client.getClientType() + ", address: "+client.getSocketAddress());
        }
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
