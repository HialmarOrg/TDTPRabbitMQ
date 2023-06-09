package ecom;

import com.google.gson.Gson;
import com.rabbitmq.client.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;

/**
 * Service de Facturation de l'application eCommerce
 */
public class ServiceFacturation {
    // échangeur des commandes émises
    private static final String EXCHANGE_EMISSION = "ecom_emission";
    // file pour les commandes en cours (pour envoyer au service commandes)
    private static final String QUEUE_ENCOURS = "ecom_encours";
    // file pour communiquer avec Banque
    private static final String QUEUE_BANQUE = "ecom_banque";
    // file pour recevoir les réponses de la Banque
    private static final String QUEUE_FACT = "ecom_fact";
    // échangeur pour les commandes valides
    private static final String EXCHANGE_VALIDE = "ecom_valide";
    // Canal pour communiquer avec RabbitMQ
    private final Channel channel;
    // Pour sérialisation JSON
    private final Gson gson = new Gson();

    /**
     * Constructeur
     * @throws Exception si problème avec RabbitMQ
     */
    public ServiceFacturation() throws Exception {
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
        // déclaration de la file pour recevoir les commandes émises
        String queueName = channel.queueDeclare().getQueue();
        // liaison à l'échangeur
        channel.queueBind(queueName, EXCHANGE_EMISSION, "", null);
        // déclaration de la file pour communiquer avec la banque
        channel.queueDeclare(QUEUE_BANQUE,false, false, false, null);
        // déclaration de la file pour recevoir les réponses de la banque
        channel.queueDeclare(QUEUE_FACT,false, false, false, null);

        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        // Callback qui traite les messages de l'échangeur de commandes émises
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            // désérialisation JSON
            Commande commande = gson.fromJson(message, Commande.class);
            // traitement commande
            traiter(commande);
        };
        // Inscription du callback sur la file liée à l'échangeur commandes émises
        channel.basicConsume(queueName, true, deliverCallback, consumerTag -> { });

        // Callback pour recevoir les réponses de la bnaque
        DeliverCallback deliverCallbackFact = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            // désérialisation JSON
            Commande commande = gson.fromJson(message, Commande.class);
            // pour savoir si c'est une facturation
            Object obj = delivery.getProperties().getHeaders().get("Facturation");
            // si l'objet est présent et que c'est un booléen
            if (obj instanceof Boolean) {
                // on traite la réponse de la banque
                traiterReponseBanque(commande, (Boolean) obj); // on le caste
            } else {
                System.err.println("Message incorrect, pas de booléen facturation.");
            }
        };
        // inscription du callback sur notre file
        channel.basicConsume(QUEUE_FACT, true, deliverCallbackFact, consumerTag -> { });

        // échangeur des commandes valides
        channel.exchangeDeclare(EXCHANGE_VALIDE, "fanout");
        // file pour recevoir les commandes valides
        String queueNameValide = channel.queueDeclare().getQueue();
        // liaison avec l'échangeur de commandes valides
        channel.queueBind(queueNameValide, EXCHANGE_VALIDE, "", null);
        // Callback pour les commandes valides
        DeliverCallback deliverCallbackValide = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            // désérialisation JSON
            Commande commande = gson.fromJson(message, Commande.class);
            // traitement commande validée
            traiterValidee(commande);
        };
        // Inscription du callback pour les commandes valides
        channel.basicConsume(queueNameValide, true, deliverCallbackValide, consumerTag -> { });
    }

    /**
     * Traitement des commandes validées
     * @param commande la commande
     * @throws IOException si problème avec RabbitMQ
     */
    private void traiterValidee(Commande commande) throws IOException {
        // on lance la facturation
        System.out.println(" [x] Facturation de '" + commande + "'");
        // sérialisation commande
        String json = gson.toJson(commande);
        // ajout de la propriété facturation à true
        AMQP.BasicProperties props = new AMQP.BasicProperties();
        // map pour les entêtes
        HashMap<String, Object> map = new HashMap<>();
        // entête facturation à true
        map.put("Facturation", true);
        // construction des propriétés
        props = props.builder().headers(map).build();
        // envoi du message à la banque
        channel.basicPublish("", QUEUE_BANQUE, props, json.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Traitement des commandes émises
     * @param commande la commande
     * @throws IOException si problème avec RabbitMQ
     */
    private void traiter(Commande commande) throws IOException {
        System.out.println(" [x] Received from Web '" + commande + "'");
        // on va transférer la commande à Banque
        String json = gson.toJson(commande);
        // On va prévenir banque que ce n'est pas une facturation réelle
        // vérif de coords bancaires
        AMQP.BasicProperties props = new AMQP.BasicProperties();
        // entêtes
        HashMap<String, Object> map = new HashMap<>();
        // entête faturation à false
        map.put("Facturation", false);
        // construction des propriétés
        props = props.builder().headers(map).build();
        // envoi du message à la banque
        channel.basicPublish("", QUEUE_BANQUE, props, json.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Traitement des réponses de banque
     * @param commande la commande
     * @param facturation est-ce une facturation ou une vérification
     * @throws IOException si problème avec RabbitMQ
     */
    private void traiterReponseBanque(Commande commande, boolean facturation) throws IOException {
        if(facturation) {
            // c'est juste une confirmation
            System.out.println("Facturation terminée de "+commande);
        } else {
            // Il faut renvoyer la réponse à Commandes
            System.out.println(" [x] Received from Banque '" + commande + "'");
            // sérialisation
            String json = gson.toJson(commande);
            // on précise qu'on est l'émetteur sur les props
            AMQP.BasicProperties props = new AMQP.BasicProperties();
            // entêtes
            HashMap<String, Object> map = new HashMap<>();
            // entête émetteur à fact
            map.put("Emetteur", "Fact");
            // construction entêtes
            props = props.builder().headers(map).build();
            // envoie vers la file de commandes en cours de traitement
            channel.basicPublish("", QUEUE_ENCOURS, props, json.getBytes(StandardCharsets.UTF_8));
        }
    }

    /**
     * Le main
     * @param argv inutilisé
     * @throws Exception si problème avec RabbitMQ
     */
    public static void main(String[] argv) throws Exception {
        new ServiceFacturation();
    }
}
