package ecom;

import bourse1.TitreBoursier;
import com.google.gson.Gson;
import com.rabbitmq.client.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.UUID;

public class ServiceWeb {

    private static final String EXCHANGE_NAME = "ecom_emission";
    private static final String QUEUE_ANNULE = "ecom_annule";
    private static final String EXCHANGE_VALIDE = "ecom_valide";
    private Channel channel;
    private Gson gson = new Gson();

    public ServiceWeb() throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        channel = connection.createChannel();
        channel.exchangeDeclare(EXCHANGE_NAME, "fanout");

        channel.queueDeclare(QUEUE_ANNULE,false, false, false, null);

        channel.exchangeDeclare(EXCHANGE_VALIDE, "fanout");

        String queueName = channel.queueDeclare().getQueue();
        channel.queueBind(queueName, EXCHANGE_VALIDE, "", null);

        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            Commande commande = gson.fromJson(message, Commande.class);
            traiterValide(commande);
        };
        channel.basicConsume(queueName, true, deliverCallback, consumerTag -> { });

        DeliverCallback deliverCallbackAnnule = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            Commande commande = gson.fromJson(message, Commande.class);
            traiterAnnule(commande);
        };
        channel.basicConsume(QUEUE_ANNULE, true, deliverCallbackAnnule, consumerTag -> { });

    }

    private void traiterAnnule(Commande commande) {
        System.out.println("Commande Annulée "+commande);
    }

    private void traiterValide(Commande commande) {
        System.out.println("Commande Valide "+commande);
    }

    private void publier(Commande commande) throws IOException {
        String json = gson.toJson(commande);
        channel.basicPublish(EXCHANGE_NAME, "", null, json.getBytes(StandardCharsets.UTF_8));
    }

    public static void main(String[] argv) throws Exception {
        ServiceWeb service = new ServiceWeb();

        for(int i=0; i<3; i++) {
            Commande commande = new Commande((int)(Math.random()*Integer.MAX_VALUE), UUID.randomUUID().toString(), UUID.randomUUID().toString(), (int)(Math.random()*100));
            service.publier(commande);
            System.out.println("Publication de "+commande);
            Thread.sleep(1000);
        }
    }
}
