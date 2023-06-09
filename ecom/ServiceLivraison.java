package ecom;

import com.google.gson.Gson;
import com.rabbitmq.client.*;

import java.nio.charset.StandardCharsets;

/**
 * Service de Livraison pour l'application eCommerce
 */
public class ServiceLivraison {

    // échangeur des commandes validées
    private static final String EXCHANGE_VALIDE = "ecom_valide";
    // pour sérialisation Json
    private final Gson gson = new Gson();

    /**
     * Constructeur
     * @throws Exception  si problème avec RabbitMQ
     */
    public ServiceLivraison() throws Exception {
        // création de l'usine à connexion
        ConnectionFactory factory = new ConnectionFactory();
        // elle fonctionne sur la machine locale
        factory.setHost("localhost");
        // création d'une connexion
        Connection connection = factory.newConnection();
        // canal pour communiquer avec RabbitMQ
        Channel channel = connection.createChannel();
        // déclaration de l'échangeur pour les commandes validées
        channel.exchangeDeclare(EXCHANGE_VALIDE, "fanout");
        // file pour recevoir les messages sur cet échangeur
        String queueName = channel.queueDeclare().getQueue();
        // liaison avec l'échangeur
        channel.queueBind(queueName, EXCHANGE_VALIDE, "", null);

        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        // Callback pour recevoir les commandes validées
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            // désérialisation
            Commande commande = gson.fromJson(message, Commande.class);
            // traitement de la commande
            traiter(commande);
        };
        // Inscription du callback
        channel.basicConsume(queueName, true, deliverCallback, consumerTag -> { });
    }

    /**
     * Traitement des commandes
     * @param commande la commande
     */
    private void traiter(Commande commande) {
        // Il faudrait les gérer ici
        System.out.println("A livrer : "+commande);
    }

    /**
     * Le main
     * @param argv inutilisé
     * @throws Exception  si problème avec RabbitMQ
     */
    public static void main(String[] argv) throws Exception {
        new ServiceLivraison();
    }
}
