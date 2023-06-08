package ecom;

import bourse1.TitreBoursier;
import com.google.gson.Gson;
import com.rabbitmq.client.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;

public class ServiceLivraison {

    private static final String EXCHANGE_NAME = "ecom_valide";

    private Channel channel;
    private Gson gson = new Gson();

    public ServiceLivraison() throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        channel = connection.createChannel();
        channel.exchangeDeclare(EXCHANGE_NAME, "fanout");


        String queueName = channel.queueDeclare().getQueue();
        channel.queueBind(queueName, EXCHANGE_NAME, "", null);

        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            System.out.println(" [x] Received '" + message + "'");
            Commande commande = gson.fromJson(message, Commande.class);
            traiter(commande);
        };
        channel.basicConsume(queueName, true, deliverCallback, consumerTag -> { });
    }

    private void traiter(Commande commande) throws IOException {
        System.out.println("A livrer : "+commande);
    }

    public static void main(String[] argv) throws Exception {
        new ServiceLivraison();
    }
}
