package usine2;

import com.google.gson.Gson;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.nio.charset.StandardCharsets;

public class UP1 {

    private static final String EXCHANGE_NAME = "usine";

    public static void main(String[] argv) throws Exception {
        Gson gson = new Gson();
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {
            channel.exchangeDeclare(EXCHANGE_NAME, "direct");

            for(int i=0; i < 50; i++) {
                Thread.sleep(1000);
                Piece p = new Piece();
                String json = gson.toJson(p);
                channel.basicPublish(EXCHANGE_NAME, "INIT", null, json.getBytes(StandardCharsets.UTF_8));
                System.out.println(" [x] Sent '" + p + "'");
            }
        }
    }
}
