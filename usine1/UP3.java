package usine1;

import com.google.gson.Gson;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.nio.charset.StandardCharsets;

public class UP3 {
    private final static String QUEUE_TRANSO = "transfo";

    public static void main(String[] argv) throws Exception {
        Gson gson = new Gson();
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.queueDeclare(QUEUE_TRANSO, false, false, false, null);
        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String json = new String(delivery.getBody(), StandardCharsets.UTF_8);
            System.out.println(json);
            Piece p = gson.fromJson(json, Piece.class);
            System.out.println(" [x] Received '" + p + "'");
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        };
        channel.basicConsume(QUEUE_TRANSO, true, deliverCallback, consumerTag -> { });
    }
}
