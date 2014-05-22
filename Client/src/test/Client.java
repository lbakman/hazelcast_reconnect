package test;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.XmlClientConfigBuilder;
import com.hazelcast.core.HazelcastInstance;
import org.apache.log4j.Logger;

import java.util.Scanner;

public class Client {
    private static Logger logger = Logger.getLogger(Client.class);

    private final HazelcastInstance hazelcast;

    public Client()
    {
        XmlClientConfigBuilder configBuilder = new XmlClientConfigBuilder();
        ClientConfig config = configBuilder.build();

        hazelcast = HazelcastClient.newHazelcastClient(config);
    }

    public void start()
    {

    }

    public void stop()
    {
        hazelcast.getLifecycleService().shutdown();
    }

    public static void main(String[] args) {
        Client client = new Client();
        client.start();

        System.out.println("Client started");

        System.out.println("Press ENTER to stop client");
        Scanner sc = new Scanner(System.in);
        sc.nextLine();
        System.out.println("Stopping client");

        client.stop();
    }
}
