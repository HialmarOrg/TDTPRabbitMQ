package ecom;

import bourse1.TitreBoursier;
import com.google.gson.Gson;
import com.rabbitmq.client.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;

public class ServiceFacturation {

    private static final String EXCHANGE_NAME = "ecom_emission";

    private static final String QUEUE_ENCOURS = "ecom_encours";
    private static final String QUEUE_BANQUE = "ecom_banque";
    private static final String QUEUE_FACT = "ecom_fact";
    private Channel channel;
    private Gson gson = new Gson();

    public ServiceFacturation() throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        channel = connection.createChannel();
        channel.exchangeDeclare(EXCHANGE_NAME, "fanout");

        String queueName = channel.queueDeclare().getQueue();
        channel.queueBind(queueName, EXCHANGE_NAME, "", null);

        channel.queueDeclare(QUEUE_BANQUE,false, false, false, null);

        channel.queueDeclare(QUEUE_FACT,false, false, false, null);

        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            System.out.println(" [x] Received '" + message + "'");
            Commande commande = gson.fromJson(message, Commande.class);
            traiter(commande);
        };
        channel.basicConsume(queueName, true, deliverCallback, consumerTag -> { });

        DeliverCallback deliverCallbackFact = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            System.out.println(" [x] Received '" + message + "'");
            Commande commande = gson.fromJson(message, Commande.class);
            traiterReponseBanque(commande);

        };
        channel.basicConsume(QUEUE_FACT, true, deliverCallbackFact, consumerTag -> { });
    }

    private void traiter(Commande commande) throws IOException {
        String json = gson.toJson(commande);
        System.out.println(" [x] Received from Web '" + commande + "'");
        channel.basicPublish("", QUEUE_BANQUE, null, json.getBytes(StandardCharsets.UTF_8));
    }

    private void traiterReponseBanque(Commande commande) throws IOException {
        System.out.println(" [x] Received from Banque '" + commande + "'");
        String json = gson.toJson(commande);
        AMQP.BasicProperties props = new AMQP.BasicProperties();
        HashMap map = new HashMap<String,Object>();
        map.put("Emetteur", "Fact");
        props = props.builder().headers(map).build();
        channel.basicPublish("", QUEUE_ENCOURS, props, json.getBytes(StandardCharsets.UTF_8));
    }

    public static void main(String[] argv) throws Exception {
        new ServiceFacturation();
    }
}
