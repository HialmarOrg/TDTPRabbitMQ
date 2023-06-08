package ecom;

import bourse1.TitreBoursier;
import com.google.gson.Gson;
import com.rabbitmq.client.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;

public class ServiceBanque {
    private static final String QUEUE_BANQUE = "ecom_banque";
    private static final String QUEUE_FACT = "ecom_fact";
    private Channel channel;
    private Gson gson = new Gson();

    public ServiceBanque() throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        channel = connection.createChannel();

        channel.queueDeclare(QUEUE_BANQUE,false, false, false, null);

        channel.queueDeclare(QUEUE_FACT,false, false, false, null);

        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            System.out.println(" [x] Received '" + message + "'");
            Commande commande = gson.fromJson(message, Commande.class);
            traiter(commande);
        };
        channel.basicConsume(QUEUE_BANQUE, true, deliverCallback, consumerTag -> { });
    }

    private void traiter(Commande commande) throws IOException {
        commande.setBanqueOk(Math.random()>0.25);
        System.out.println("Banque : OK? "+commande.isBanqueOk() + " pour la commande "+commande.getId());
        String json = gson.toJson(commande);
        channel.basicPublish("", QUEUE_FACT, null, json.getBytes(StandardCharsets.UTF_8));
    }


    public static void main(String[] argv) throws Exception {
        new ServiceBanque();
    }
}
