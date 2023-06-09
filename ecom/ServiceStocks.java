package ecom;

import com.google.gson.Gson;
import com.rabbitmq.client.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;

/**
 * Service de gestion des stocks de l'application eCommerce
 */
public class ServiceStocks {
    // échangeur des commandes émises par le service web
    private static final String EXCHANGE_EMISSION = "ecom_emission";
    // file pour envoyer les commandes en cours de traitement
    private static final String QUEUE_ENCOURS = "ecom_encours";
    // échangeur des commandes validées par le service de gestion des commandes
    private static final String EXCHANGE_VALIDE = "ecom_valide";
    // canal pour communiquer avec RabbitMQ
    private final Channel channel;
    // sérialisation JSON
    private final Gson gson = new Gson();

    /**
     * Constructeur
     * @throws Exception  si problème avec RabbitMQ
     */
    public ServiceStocks() throws Exception {
        // création de l'usine à connexion
        ConnectionFactory factory = new ConnectionFactory();
        // elle fonctionne sur la machine locale
        factory.setHost("localhost");
        // création d'une connexion
        Connection connection = factory.newConnection();
        // création du canal
        channel = connection.createChannel();
        // déclaration de l'échangeur pour les commandes émises
        channel.exchangeDeclare(EXCHANGE_EMISSION, "fanout");
        // file pour recevoir les messages de cet échangeur
        String queueName = channel.queueDeclare().getQueue();
        // liaison avec l'échangeur
        channel.queueBind(queueName, EXCHANGE_EMISSION, "", null);

        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        // Callback de réception des commandes émises
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            // Désérialisation
            Commande commande = gson.fromJson(message, Commande.class);
            // Traitement des commandes
            traiter(commande);
        };
        // inscription du callback
        channel.basicConsume(queueName, true, deliverCallback, consumerTag -> { });

        // Déclaration de l'échangeur pour les commandes validées
        channel.exchangeDeclare(EXCHANGE_VALIDE, "fanout");
        // file pour recevoir les messages de cet échangeur
        String queueNameValide = channel.queueDeclare().getQueue();
        // liaison avec l'échangeur
        channel.queueBind(queueNameValide, EXCHANGE_VALIDE, "", null);
        // Callback pour recevoir les commandes validées
        DeliverCallback deliverCallbackValide = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            // désérialisation JSON
            Commande commande = gson.fromJson(message, Commande.class);
            // traitement de la commande
            traiterValidee(commande);
        };
        // inscription du callback
        channel.basicConsume(queueNameValide, true, deliverCallbackValide, consumerTag -> { });
    }

    /**
     * Traitement d'une commande validée
     * @param commande la commande
     */
    private void traiterValidee(Commande commande) {
        // Il faudrait vraiment décrémenter les stocks
        System.out.println("Décrémentation des stocks : "+commande);
    }

    /**
     * Traitement d'une commande émise
     * @param commande la commande
     * @throws IOException si problème avec RabbitMQ
     */
    private void traiter(Commande commande) throws IOException {
        // vérification des stocks (aléatoire pour l'instant)
        commande.setStockOk(Math.random()>0.25);
        System.out.println("Stocks : OK? "+commande.isStockOk() + " pour la commande "+commande.getId());
        // on envoie la réponse au service de gestion des commandes
        String json = gson.toJson(commande);
        // On précise l'émetteur en propriétés
        AMQP.BasicProperties props = new AMQP.BasicProperties();
        // entêtes
        HashMap<String, Object> map = new HashMap<>();
        // entête émetteur à Stocks
        map.put("Emetteur", "Stocks");
        // constrcution des props
        props = props.builder().headers(map).build();
        // envoi de la réponse sur la file des commandes en cours de traitement
        channel.basicPublish("", QUEUE_ENCOURS, props, json.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Le main
     * @param argv inutilisé
     * @throws Exception si problème avec RabbitMQ
     */
    public static void main(String[] argv) throws Exception {
        new ServiceStocks();
    }
}
