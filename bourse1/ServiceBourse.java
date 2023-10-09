package bourse1;

import bourse_tp.TitreBoursier;
import com.google.gson.Gson;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class ServiceBourse {
    private static final String EXCHANGE_NAME = "bourse";
    private final Channel channel;
    private final Gson gson = new Gson();

    public ServiceBourse() throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        channel = connection.createChannel();
        channel.exchangeDeclare(EXCHANGE_NAME, "fanout");
    }

    private void publier(TitreBoursier titreBoursier) throws IOException {
        String json = gson.toJson(titreBoursier);
        channel.basicPublish(EXCHANGE_NAME, "", null, json.getBytes(StandardCharsets.UTF_8));
    }

    public static void main(String[] argv) throws Exception {
        ServiceBourse service = new ServiceBourse();

        TitreBoursier google = new TitreBoursier("GOOG", "Google Inc.", 391.03f, "USD", 0.0f, 0.0f);
        TitreBoursier microsoft = new TitreBoursier("MSFT", "Microsoft Corp.",  25.79f, "USD", 0.0f, 0.0f);

        for(int i=0; i<100; i++) {
            float variation = (float)Math.random() * 20.0f - 10.0f;
            google.setVariation(variation);
            service.publier(google);
            System.out.println("Publication de "+google);
            variation = (float)Math.random() * 20.0f - 10.0f;
            microsoft.setVariation(variation);
            service.publier(microsoft);
            System.out.println("Publication de "+microsoft);
            Thread.sleep(1000);
        }
    }
}
