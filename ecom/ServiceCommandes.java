package ecom;

import com.google.gson.Gson;
import com.rabbitmq.client.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * Service commande de l'application eCommerce
 */
public class ServiceCommandes {
    // file des commandes en cours de traitement (utilisée par stocks et facturation)
    private static final String QUEUE_ENCOURS = "ecom_encours";
    // file des commandes annulées (pour prévenir Web)
    private static final String QUEUE_ANNULE = "ecom_annule";
    // échangeur des commandes valides (prévient Web, Stocks, Facturation et Livraison)
    private static final String EXCHANGE_VALIDE = "ecom_valide";
    // Canal pour interagir avec RabbitMQ
    private final Channel channel;
    // Pour la sérialisation JSON
    private final Gson gson = new Gson();

    // Stocke les commandes
    private final Map<Integer, Commande> commandeMap = new HashMap<>();
    // Permet de savoir si stocks a déjà répondu
    private final Map<Integer, Boolean> reponseStocks = new HashMap<>();
    // Permet de savoir si facturation a déjà répondu
    private final Map<Integer, Boolean> reponseBanque = new HashMap<>();

    /**
     * Constructeur
     * @throws Exception si problème avec RabbitMQ
     */
    public ServiceCommandes() throws Exception {
        // création de l'usine à connexion
        ConnectionFactory factory = new ConnectionFactory();
        // elle fonctionne sur la machine locale
        factory.setHost("localhost");
        // création d'une connexion
        Connection connection = factory.newConnection();
        // création du canal
        channel = connection.createChannel();
        // déclaration de notre file
        channel.queueDeclare(QUEUE_ENCOURS,false, false, false, null);
        // déclaration de la file pour les commandes annulées
        channel.queueDeclare(QUEUE_ANNULE,false, false, false, null);
        // déclaration de l'échangeur pour les commandes valides
        channel.exchangeDeclare(EXCHANGE_VALIDE, "fanout");

        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        // Callback pour nos messages
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            // Désérialisation JSON
            Commande commande = gson.fromJson(message, Commande.class);
            // Est-ce qu'on a une info sur l'émetteur
            Object obj = delivery.getProperties().getHeaders().get("Emetteur");
            // Traitons la commande
            traiter(commande, obj.toString());
        };
        channel.basicConsume(QUEUE_ENCOURS, true, deliverCallback, consumerTag -> { });
    }

    /**
     * Traitement de la commande
     * @param commande la commande
     * @param emetteur émetteur du message (Stocks ou Fact)
     * @throws IOException si problème avec RabbitMQ
     */
    private void traiter(Commande commande, String emetteur) throws IOException {
        System.out.println("Traiter "+commande+" Emetteur "+emetteur);
        // si on doit envoyer un message
        boolean fini = false;
        // si la commande est valide
        boolean valide = false;
        switch (emetteur) {
            case "Stocks" -> { // message de stocks
                System.out.println("Reception réponse stocks pour commande " + commande.getId());
                // on note qu'on a reçu la réponse des stocks pour cette commande
                reponseStocks.put(commande.getId(), true);
                // si on a déjà reçu la réponse de facturation
                if (reponseBanque.containsKey(commande.getId())) {
                    // on a fini avec cette commande
                    fini = true;
                    // on recopie la réponse de stocks
                    commandeMap.get(commande.getId()).setStockOk(commande.isStockOk());
                    // la commande est valide si la réponse de stock est ok et
                    // si l'ancienne réponse de banque est ok
                    valide = commande.isStockOk() && commandeMap.get(commande.getId()).isBanqueOk();
                    System.out.println("Fini, valide ? " + valide + " " + commandeMap.get(commande.getId()));
                } else {
                    // on stocke la commande
                    commandeMap.put(commande.getId(), commande);
                    // on a reçu la réponse des stocks
                    reponseStocks.put(commande.getId(), true);
                    // la commande est toujours en cours de traitement
                    System.out.println("En cours " + commande);
                }
            }
            case "Fact" -> { // message de Fact
                System.out.println("Reception réponse facturation pour commande " + commande.getId());
                // on note qu'on a reçu la réponse de banque pour cette commande
                reponseBanque.put(commande.getId(), true);
                // si on a déjà reçu la réponse des stocks
                if (reponseStocks.containsKey(commande.getId())) {
                    // on a fini avec cette commande
                    fini = true;
                    // on recopie la réponse de facturation
                    commandeMap.get(commande.getId()).setBanqueOk(commande.isBanqueOk());
                    // la commande est valide si la réponse de facturation est ok et
                    // si l'ancienne réponse de stocks est ok
                    valide = commande.isBanqueOk() && commandeMap.get(commande.getId()).isStockOk();
                    System.out.println("Fini, valide ? " + valide + " " + commandeMap.get(commande.getId()));
                } else {
                    // on stocke la commande
                    commandeMap.put(commande.getId(), commande);
                    // on a reçu la réponse de stocks
                    reponseBanque.put(commande.getId(), true);
                    // la commande est toujours en cours
                    System.out.println("En cours " + commande);
                }
            }
            default -> {
                System.err.println("Message incorrect, pas d'émetteur ou émetteur inconnu : " + emetteur);
                // il ne faut pas envoyer de message
                return;
            }
        }
        // Si on a fini de traiter cette commande
        if(fini) {
            // on la sérialise
            String json = gson.toJson(commandeMap.get(commande.getId()));
            // si elle est valide on l'envoie via l'échangeur valide sinon via la file annulation
            if (valide)
                channel.basicPublish(EXCHANGE_VALIDE, "", null, json.getBytes(StandardCharsets.UTF_8));
            else
                channel.basicPublish("", QUEUE_ANNULE, null, json.getBytes(StandardCharsets.UTF_8));
        }
    }

    /**
     * Le main
     * @param argv inutilisé
     * @throws Exception si problème avec RabbitMQ
     */
    public static void main(String[] argv) throws Exception {
        new ServiceCommandes();
    }
}
