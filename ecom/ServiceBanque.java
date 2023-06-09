package ecom;

import com.google.gson.Gson;
import com.rabbitmq.client.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;

/**
 * Service bancaire pour l'application eCommerce
 */
public class ServiceBanque {
    // file de la banque (utilisée par facturation)
    private static final String QUEUE_BANQUE = "ecom_banque";
    // file de facturation (utilisée par la banque et par le service de commandes)
    private static final String QUEUE_FACT = "ecom_fact";
    // canal pour interagir avec RabbitMQ
    private final Channel channel;
    // utilisé pour la (dé)sérialisation en JSON
    private final Gson gson = new Gson();

    /**
     * Constructeur
     * @throws Exception s'il y a un problème avec RabbitMQ
     */
    public ServiceBanque() throws Exception {
        // création de l'usine à connexion
        ConnectionFactory factory = new ConnectionFactory();
        // elle fonctionne sur la machine locale
        factory.setHost("localhost");
        // création d'une connexion
        Connection connection = factory.newConnection();
        // création du canal
        channel = connection.createChannel();
        // déclaration de notre file
        channel.queueDeclare(QUEUE_BANQUE,false, false, false, null);
        // déclaration de la file de facturation
        channel.queueDeclare(QUEUE_FACT,false, false, false, null);

        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        // Callback pour traiter les messages envoyés par facturation
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            // désérialisation JSON
            Commande commande = gson.fromJson(message, Commande.class);
            // on veut savoir s'il s'agit d'une facturation ou d'une vérification des coords bancaires
            Object obj = delivery.getProperties().getHeaders().get("Facturation");
            // si l'obj est ok
            if (obj instanceof Boolean) {
                // on va traiter la commande
                traiter(commande, (Boolean) obj); // on le caste
            } else {
                System.err.println("Message incorrect, pas de booléen facturation.");
            }
        };
        // Inscription du callback (auto acquittement des messages et pas de traitement d'annulation)
        channel.basicConsume(QUEUE_BANQUE, true, deliverCallback, consumerTag -> { });
    }

    /**
     * Traitement d'une commande
     * @param commande la commande
     * @param facturation est-ce qu'on facture ou on vérifie les infos bancaires
     * @throws IOException si problème avec RabbitMQ
     */
    private void traiter(Commande commande, boolean facturation) throws IOException {
        if (facturation) {
            // on ne fait rien ici (il faudrait vraiment facturer)
            System.out.println("Facturation de : " + commande);
        } else {
            // on vérifie les coords bancaires (aléatoirement ok/ko)
            commande.setBanqueOk(Math.random() > 0.25);
            System.out.println("Banque : OK? " + commande.isBanqueOk() + " pour la commande " + commande.getId());
        }
        // sérialisation JSON
        String json = gson.toJson(commande);
        // ajout de la propriété permettant de savoir si c'était une facturation
        AMQP.BasicProperties props = new AMQP.BasicProperties();
        // hashmap pour les entêtes
        HashMap<String, Object> map = new HashMap<>();
        // entête facturation
        map.put("Facturation", facturation);
        // construction des propriétés
        props = props.builder().headers(map).build();
        // on transmet la commande à facturation
        channel.basicPublish("", QUEUE_FACT, props, json.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Le main
     * @param argv pas utilisé
     * @throws Exception s'il y a un problème avec RabbitMQ
     */
    public static void main(String[] argv) throws Exception {
        new ServiceBanque();
    }
}
