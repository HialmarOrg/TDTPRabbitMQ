package bourse2;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.nio.charset.StandardCharsets;

/**
 * Courtier qui reçoit tout
 */
public class Courtier1 {

    /**
     * Le nom de l'échangeur qui publie les cours
     */
    private static final String EXCHANGE_NAME = "bourse_headers";

    /**
     * Le main
     * @param argv arguments inutilisés
     * @throws Exception en cas de problème
     */
    public static void main(String[] argv) throws Exception {
        // Usine à connexions
        ConnectionFactory factory = new ConnectionFactory();
        // On travaille avec un broker sur la machine locale
        factory.setHost("localhost");
        // Connexion
        Connection connection = factory.newConnection();
        // Canal de communication
        Channel channel = connection.createChannel();

        // Déclaration de l'échangeur
        channel.exchangeDeclare(EXCHANGE_NAME, "headers", true);
        // File pour recevoir les publications
        String queueName = channel.queueDeclare().getQueue();
        // Liaison entre l'échangeur et la file
        channel.queueBind(queueName, EXCHANGE_NAME, "", null);

        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        // Callback pour la réception des cours
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            // on récupère la chaine en corps et on l'affiche
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            System.out.println(" [x] Received '" + message + "'");
        };
        // Inscription du callback
        channel.basicConsume(queueName, true, deliverCallback, consumerTag -> { });
    }
}
