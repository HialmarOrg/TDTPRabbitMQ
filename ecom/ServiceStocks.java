package ecom;

import bourse1.TitreBoursier;
import com.google.gson.Gson;
import com.rabbitmq.client.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Random;

public class ServiceStocks {

    private static final String EXCHANGE_NAME = "ecom_emission";

    private static final String QUEUE_NAME = "ecom_encours";
    private Channel channel;
    private Gson gson = new Gson();

    public ServiceStocks() throws Exception {
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
            Commande commande = gson.fromJson(message, Commande.class);
            traiter(commande);
        };
        channel.basicConsume(queueName, true, deliverCallback, consumerTag -> { });
    }

    private void traiter(Commande commande) throws IOException {
        commande.setStockOk(Math.random()>0.25);
        System.out.println("Stocks : OK? "+commande.isStockOk() + " pour la commande "+commande.getId());
        String json = gson.toJson(commande);
        AMQP.BasicProperties props = new AMQP.BasicProperties();
        HashMap map = new HashMap<String,Object>();
        map.put("Emetteur", "Stocks");
        props = props.builder().headers(map).build();
        channel.basicPublish("", QUEUE_NAME, props, json.getBytes(StandardCharsets.UTF_8));
    }

    public static void main(String[] argv) throws Exception {
        new ServiceStocks();
    }
}
