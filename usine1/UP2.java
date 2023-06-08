package usine1;

import com.google.gson.Gson;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.nio.charset.StandardCharsets;
import java.util.Date;

public class UP2 {

    private final static String QUEUE_INIT = "init";
    private final static String QUEUE_TRANSO = "transfo";

    public static void main(String[] argv) throws Exception {
        Gson gson = new Gson();
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.queueDeclare(QUEUE_INIT, false, false, false, null);
        channel.queueDeclare(QUEUE_TRANSO, false, false, false, null);
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
            channel.basicPublish("", QUEUE_TRANSO, null, json.getBytes(StandardCharsets.UTF_8));
            System.out.println(" [x] Sent '" + p + "'");
        };
        channel.basicConsume(QUEUE_INIT, true, deliverCallback, consumerTag -> { });
    }
}
