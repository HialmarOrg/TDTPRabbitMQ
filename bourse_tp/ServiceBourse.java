package bourse_tp;

import com.google.gson.Gson;
import com.rabbitmq.client.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;

/**
 * Serveur AMQP pour la Bourse
 */
public class ServiceBourse {

    // Échangeur pour la publication des cours
    private static final String EXCHANGE_NAME = "bourse_headers";
    // Canal avec le broker
    private final Channel channel;
    // Convertisseur JSON
    private final Gson gson = new Gson();

    // Stockage des titres boursiers
    private final HashMap<String, TitreBoursier> titres = new HashMap<>();

    /**
     * Constructeur
     * @throws Exception en cas de pb de communication avec le broker
     */
    public ServiceBourse() throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        // on se connecte au broker local à la machine
        factory.setHost("localhost");
        // connexion et canal
        Connection connection = factory.newConnection();
        channel = connection.createChannel();
        // Déclaration de l'échangeur pour les cours
        channel.exchangeDeclare(EXCHANGE_NAME, "headers", true);
        // File pour cet échangeur
        String queueName = channel.queueDeclare().getQueue();
        // Liaison entre les deux
        channel.queueBind(queueName, EXCHANGE_NAME, "", null);

        System.out.println(" Service Bourse [*] Waiting for messages. To exit press CTRL+C");

        // Callback pour vérifier que les cours sont bien envoyés
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            System.out.println(" Service Bourse [x] Received '" + message + "'");
        };
        // Inscription du callback
        channel.basicConsume(queueName, true, deliverCallback, consumerTag -> { });
        // File pour les RPC
        channel.queueDeclare("bourse_rpc", true, false, false, null);
        // Callback pour les RPC
        // on les traite dans gestionRPC
        DeliverCallback deliverCallbackRPC = this::gestionRPC;
        // Inscription du callback pour les RPC
        channel.basicConsume("bourse_rpc", true, deliverCallbackRPC, consumerTag -> { });
    }

    /**
     * Gestion des RPC
     * @param consumerTag : le tag du callback (inutilisé)
     * @param delivery : le message
     */
    private void gestionRPC(String consumerTag, Delivery delivery) {
        // On récupère la chaine
        String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
        System.out.println(" [x] Received RPC '" + message + "'");
        // On récupère le type d'opération
        String op = delivery.getProperties().getHeaders().get("OP").toString();
        System.out.println(op);
        System.out.println("Reception RPC Op "+op);
        // On retransforme le type en valeur d'énumération
        OperationType operationType = OperationType.valueOf(op);
        // On convertit le corps en TitreBoursier
        TitreBoursier titreBoursier = gson.fromJson(message, TitreBoursier.class);
        System.out.println(titreBoursier);
        switch(operationType){
            case CREATE -> this.createTitre(titreBoursier, delivery.getProperties().getReplyTo(), delivery.getProperties().getCorrelationId());
            case UPDATE -> this.updateTitre(titreBoursier, delivery.getProperties().getReplyTo(), delivery.getProperties().getCorrelationId());
            case DELETE -> this.deleteTitre(titreBoursier, delivery.getProperties().getReplyTo(), delivery.getProperties().getCorrelationId());
            case REQUEST -> this.getTitre(titreBoursier, delivery.getProperties().getReplyTo(), delivery.getProperties().getCorrelationId());
        }

    }

    /**
     * Mises à jour d'un titre (avec calcul de la variation)
     * @param titreBoursier : le titre
     * @return le nouveau titre
     */
    public TitreBoursier updateTitre(TitreBoursier titreBoursier) {
        // on tente de récupérer l'ancienne version
        TitreBoursier old = titres.get(titreBoursier.getMnemo());
        if (old != null) {
            // on en avait une
            System.out.println("Update old "+ old);
            // calcul de la variation
            titreBoursier.setVariationPourcent((titreBoursier.getValeur() - old.getValeur()) / old.getValeur() * 100.0f);
            titreBoursier.setVariation(titreBoursier.getValeur() - old.getValeur());
        }
        System.out.println("Update new "+ titreBoursier);
        // On met à jour
        titres.put(titreBoursier.getMnemo(), titreBoursier);
        // On renvoie le titre à jour avec la variation
        return titreBoursier;
    }

    /**
     * Récupération d'un titre
     * @param mnemonic : le mnémonique du titre
     * @return le titre ou null
     */
    public TitreBoursier getFromTitres(String mnemonic) {
        return titres.get(mnemonic);
    }

    /**
     * Suppression d'un titre
     * @param mnemonic : le mnémonique du titre
     */
    public void deleteFromTitres(String mnemonic) {
        titres.remove(mnemonic);
    }

    /**
     * Gestion de la demande d'un titre
     * @param titreBoursier : le titre demandé (seul le mnémonique est utilisé)
     * @param fileReponse : le nom de la file pour répondre
     * @param correlationId : l'id de corrélation qui doit être recopié sur la réponse
     */
    private void getTitre(TitreBoursier titreBoursier, String fileReponse, String correlationId) {
        // on le cherche
        TitreBoursier titreBoursierComplet = this.getFromTitres(titreBoursier.getMnemo());
        // s'il n'est pas null
        if (titreBoursierComplet != null) {
            // conversion en JSON
            String json = gson.toJson(titreBoursierComplet);
            // propriétés pour l'id de corrélation
            AMQP.BasicProperties props = new AMQP.BasicProperties();
            props = props.builder().correlationId(correlationId).build();
            try {
                // on envoie la réponse
                channel.basicPublish("", fileReponse, props, json.getBytes(StandardCharsets.UTF_8));
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            // propriétés pour l'id de corrélation
            AMQP.BasicProperties props = new AMQP.BasicProperties();
            props = props.builder().correlationId(correlationId).build();
            try {
                // on envoie une réponse erreur Titre Inconnu
                String json = gson.toJson(new ResponseMessage("Titre Inconnu"));
                channel.basicPublish("", fileReponse, props, json.getBytes(StandardCharsets.UTF_8));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    /**
     * Gestion de la suppression d'un titre
     * @param titreBoursier : le titre à supprimer (seul le mnémonique est utilisé)
     * @param fileReponse : le nom de la file pour répondre
     * @param correlationId : l'id de corrélation qui doit être recopié sur la réponse
     */
    private void deleteTitre(TitreBoursier titreBoursier, String fileReponse, String correlationId) {
        // On tente de le supprimer
        // s'il n'y est pas on ne fera rien (idempotent)
        this.deleteFromTitres(titreBoursier.getMnemo());
        // propriétés pour l'id de corrélation
        AMQP.BasicProperties props = new AMQP.BasicProperties();
        props = props.builder().correlationId(correlationId).build();
        try {
            // on envoie une réponse Done
            String json = gson.toJson(new ResponseMessage("Done"));
            channel.basicPublish("", fileReponse, props, json.getBytes(StandardCharsets.UTF_8));
            // on publie la suppression
            publier(titreBoursier, OperationType.DELETE);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * Gestion de la mise à jour d'un titre
     * @param titreBoursier : le titre à modifier
     * @param fileReponse : le nom de la file pour répondre
     * @param correlationId : l'id de corrélation qui doit être recopié sur la réponse
     */
    private void updateTitre(TitreBoursier titreBoursier, String fileReponse, String correlationId) {
        // on le met à jour et on récupère la nouvelle version (note : s'il n'existait pas ça le crée)
        titreBoursier = this.updateTitre(titreBoursier);
        // propriétés pour l'id de corrélation
        AMQP.BasicProperties props = new AMQP.BasicProperties();
        props = props.builder().correlationId(correlationId).build();
        try {
            // on envoie une réponse Done
            String json = gson.toJson(new ResponseMessage("Done"));
            channel.basicPublish("", fileReponse, props, json.getBytes(StandardCharsets.UTF_8));
            // on publie la mise à jour
            publier(titreBoursier, OperationType.UPDATE);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Gestion de la création d'un titre
     * @param titreBoursier : le titre à créer
     * @param fileReponse : le nom de la file pour répondre
     * @param correlationId : l'id de corrélation qui doit être recopié sur la réponse
     */
    private void createTitre(TitreBoursier titreBoursier, String fileReponse, String correlationId) {
        // on le crée (note : s'il existait on le met à jour et on récupère la nouvelle version)
        titreBoursier = this.updateTitre(titreBoursier);
        // propriétés pour l'id de corrélation
        AMQP.BasicProperties props = new AMQP.BasicProperties();
        props = props.builder().correlationId(correlationId).build();
        try {
            // on envoie une réponse Done
            String json = gson.toJson(new ResponseMessage("Done"));
            channel.basicPublish("", fileReponse, props, json.getBytes(StandardCharsets.UTF_8));
            // on publie la création
            publier(titreBoursier, OperationType.CREATE);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Publication des opérations sur les titres vers toutes les applications abonnées
     * @param titreBoursier : le titre à publier
     * @param operationType : l'opération indiquée
     * @throws IOException : en cas de pb d'E/S avec le broker
     */
    private void publier(TitreBoursier titreBoursier, OperationType operationType) throws IOException {
        // conversion en JSON
        String json = gson.toJson(titreBoursier);
        // propriétés
        AMQP.BasicProperties props = new AMQP.BasicProperties();
        HashMap<String, Object> map = new HashMap<>();
        // Mnémonique
        map.put(titreBoursier.getMnemo(), "TRUE");
        // Opération
        map.put("OP", operationType.name());
        props = props.builder().headers(map).build();
        // publication
        channel.basicPublish(EXCHANGE_NAME, "", props, json.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Le Main
     * @param argv arguments (inutilisés)
     * @throws Exception en cas de pb
     */
    public static void main(String[] argv) throws Exception {
        // Création du service
        ServiceBourse service = new ServiceBourse();

        // Titres de démo
        TitreBoursier google = new TitreBoursier("GOOG", "Google Inc.", 391.03f, "USD", 0.0f, 0.0f);
        TitreBoursier microsoft = new TitreBoursier("MSFT", "Microsoft Corp.", 25.79f, "USD", 0.0f, 0.0f);
        service.updateTitre(google);
        service.updateTitre(microsoft);

        // Publication de leur création
        service.publier(google, OperationType.CREATE);
        System.out.println("Publication de "+google);
        service.publier(microsoft, OperationType.CREATE);
        System.out.println("Publication de "+microsoft);

        // Attente de 10 secondes
        Thread.sleep(10000);

        while(true) {
            // On publie toutes les 10 secondes
            service.publishAll();
            Thread.sleep(10000);
        }
    }

    /**
     * Publie tous les titres connus
     */
    private void publishAll() {
        // On parcourt la table des titres côté valeurs
        for (TitreBoursier titreBoursier: titres.values()) {
            try {
                // On publie
                publier(titreBoursier, OperationType.UPDATE);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
