package usine1;

import com.google.gson.Gson;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.nio.charset.StandardCharsets;

public class UP1 {

    private final static String QUEUE_INIT = "init";

    public static void main(String[] argv) throws Exception {
        Gson gson = new Gson();
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {
            channel.queueDeclare(QUEUE_INIT, false, false, false, null);

            for(int i=0; i < 50; i++) {
                Thread.sleep(1000);
                Piece p = new Piece();
                String json = gson.toJson(p);
                channel.basicPublish("", QUEUE_INIT, null, json.getBytes(StandardCharsets.UTF_8));
                System.out.println(" [x] Sent '" + p + "'");
            }

        }
    }
}
