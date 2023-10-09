package bourse_tp;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;

/**
 * Courtier uniquement intéressé par Google
 */
public class ClientCourtier2 {

    private static final String EXCHANGE_NAME = "bourse_headers";

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.exchangeDeclare(EXCHANGE_NAME, "headers", true);
        String queueName = channel.queueDeclare().getQueue();
        HashMap<String, Object> map = new HashMap<>();
        map.put("x-match","any");
        map.put("GOOG","TRUE"); // ici, on précise qu'on veut du Google
        channel.queueBind(queueName, EXCHANGE_NAME, "", map);

        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            System.out.println(" [x] Received '" + message + "'");
        };
        channel.basicConsume(queueName, true, deliverCallback, consumerTag -> { });
    }
}
