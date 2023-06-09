package ecom;

import com.google.gson.Gson;
import com.rabbitmq.client.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

/**
 * Service Web de l'application eCommerce
 */
public class ServiceWeb {

    // échangeur pour les commandes émises (on l'utilise pour les transmettre à Stocks et Fact)
    private static final String EXCHANGE_EMISSION = "ecom_emission";
    // file pour les commandes annulées (envoyées par la gestion des commandes)
    private static final String QUEUE_ANNULE = "ecom_annule";
    // échangeur pour les commandes validées (envoyées par la gestion des commandes)
    private static final String EXCHANGE_VALIDE = "ecom_valide";
    // canal pour communiquer avec RabbitMQ
    private final Channel channel;
    // pour la sérialisation JSON
    private final Gson gson = new Gson();

    /**
     * Constructeur
     * @throws Exception si problème avec RabbitMQ
     */
    public ServiceWeb() throws Exception {
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
        // déclaration de la file pour les commandes annulées
        channel.queueDeclare(QUEUE_ANNULE,false, false, false, null);
        // déclaration de l'échangeur pour les commandes validées
        channel.exchangeDeclare(EXCHANGE_VALIDE, "fanout");
        // file pour cet échangeur
        String queueName = channel.queueDeclare().getQueue();
        // liaison de la file à l'échangeur
        channel.queueBind(queueName, EXCHANGE_VALIDE, "", null);

        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        // Callback pour recevoir les commandes validées
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            // désérialisation
            Commande commande = gson.fromJson(message, Commande.class);
            // traitement
            traiterValide(commande);
        };
        // inscription du callback
        channel.basicConsume(queueName, true, deliverCallback, consumerTag -> { });

        // Callback pour recevoir les commandes annulées
        DeliverCallback deliverCallbackAnnule = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            // désérialisation
            Commande commande = gson.fromJson(message, Commande.class);
            // traitement
            traiterAnnule(commande);
        };
        channel.basicConsume(QUEUE_ANNULE, true, deliverCallbackAnnule, consumerTag -> { });

    }

    /**
     * Traitement des commandes annulées
     * @param commande la commande
     */
    private void traiterAnnule(Commande commande) {
        // Il faudrait les traiter ici
        System.out.println("Commande Annulée "+commande);
    }

    /**
     * Traitement des commandes validées
     * @param commande la commande
     */
    private void traiterValide(Commande commande) {
        // Il faudrait les traiter ici
        System.out.println("Commande Valide "+commande);
    }

    /**
     * Publication des commandes
     * @param commande la commande
     * @throws IOException si problème avec RabbitMQ
     */
    private void publier(Commande commande) throws IOException {
        // sérialisation
        String json = gson.toJson(commande);
        // envoi sur l'échangeur des commandes émises
        channel.basicPublish(EXCHANGE_EMISSION, "", null, json.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Le main
     * @param argv inutilisé
     * @throws Exception si problème avec RabbitMQ
     */
    public static void main(String[] argv) throws Exception {
        // Création du service web
        ServiceWeb service = new ServiceWeb();

        // boucle d'envoi de commandes de test
        for(int i=0; i<3; i++) {
            // Création de la commande avec des données aléatoires
            Commande commande = new Commande((int)(Math.random()*Integer.MAX_VALUE), UUID.randomUUID().toString(), UUID.randomUUID().toString(), (int)(Math.random()*100));
            // On envoi la commande
            service.publier(commande);
            System.out.println("Publication de "+commande);
            // On attend 1 seconde
            Thread.sleep(1000);
        }
    }
}
