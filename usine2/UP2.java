package usine2;

import com.google.gson.Gson;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.nio.charset.StandardCharsets;
import java.util.Date;

public class UP2 {

    private static final String EXCHANGE_NAME = "usine";

    public static void main(String[] argv) throws Exception {
        Gson gson = new Gson();
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.exchangeDeclare(EXCHANGE_NAME, "direct");

        String queueName = channel.queueDeclare().getQueue();
        channel.queueBind(queueName, EXCHANGE_NAME, "INIT");

        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String json = new String(delivery.getBody(), StandardCharsets.UTF_8);
            Piece p = gson.fromJson(json, Piece.class);
            System.out.println(" [x] Received '" + p + "'");
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            p.setTansforme(true); p.setDateModification(new Date());
            json = gson.toJson(p);
            channel.basicPublish(EXCHANGE_NAME, "TRANSFO", null, json.getBytes(StandardCharsets.UTF_8));
            System.out.println(" [x] Sent '" + p + "'");
        };
        channel.basicConsume(queueName, true, deliverCallback, consumerTag -> { });
    }
}
