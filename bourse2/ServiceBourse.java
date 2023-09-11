package bourse2;

import bourse1.TitreBoursier;
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
        DeliverCallback deliverCallbackRPC = (consumerTag, delivery) -> {
            // on les traite ailleurs
            this.gestionRPC(consumerTag, delivery);
        };
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

    public TitreBoursier updateTitre(TitreBoursier titreBoursier) {
        TitreBoursier old = titres.get(titreBoursier.getMnemo());
        if (old != null) {
            System.out.println("Update old "+ old);
            // calcul de la variation
            titreBoursier.setVariation((titreBoursier.getCours() - old.getCours()) / old.getCours() * 100.0f);
        }
        System.out.println("Update new "+ titreBoursier);
        titres.put(titreBoursier.getMnemo(), titreBoursier);

        return titreBoursier;
    }

    public TitreBoursier getFromTitres(String mnemonic) {
        return titres.get(mnemonic);
    }

    public void deleteFromTitres(String mnemonic) {
        titres.remove(mnemonic);
    }

    private void getTitre(TitreBoursier titreBoursier, String fileReponse, String correlationId) {
        TitreBoursier titreBoursierComplet = this.getFromTitres(titreBoursier.getMnemo());
        if (titreBoursierComplet != null) {
            String json = gson.toJson(titreBoursierComplet);
            AMQP.BasicProperties props = new AMQP.BasicProperties();
            props = props.builder().correlationId(correlationId).build();
            try {
                channel.basicPublish("", fileReponse, props, json.getBytes(StandardCharsets.UTF_8));
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            AMQP.BasicProperties props = new AMQP.BasicProperties();
            props = props.builder().correlationId(correlationId).build();
            try {
                String json = gson.toJson(new ResponseMessage("Titre Inconnu"));
                channel.basicPublish("", fileReponse, props, json.getBytes(StandardCharsets.UTF_8));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    private void deleteTitre(TitreBoursier titreBoursier, String fileReponse, String correlationId) {
        this.deleteFromTitres(titreBoursier.getMnemo());
        AMQP.BasicProperties props = new AMQP.BasicProperties();
        props = props.builder().correlationId(correlationId).build();
        try {
            String json = gson.toJson(new ResponseMessage("Done"));
            channel.basicPublish("", fileReponse, props, json.getBytes(StandardCharsets.UTF_8));
            publier(titreBoursier, OperationType.DELETE);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void updateTitre(TitreBoursier titreBoursier, String fileReponse, String correlationId) {
        titreBoursier = this.updateTitre(titreBoursier);
        AMQP.BasicProperties props = new AMQP.BasicProperties();
        props = props.builder().correlationId(correlationId).build();
        try {
            String json = gson.toJson(new ResponseMessage("Done"));
            channel.basicPublish("", fileReponse, props, json.getBytes(StandardCharsets.UTF_8));
            publier(titreBoursier, OperationType.UPDATE);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void createTitre(TitreBoursier titreBoursier, String fileReponse, String correlationId) {
        this.updateTitre(titreBoursier);
        AMQP.BasicProperties props = new AMQP.BasicProperties();
        props = props.builder().correlationId(correlationId).build();
        try {
            String json = gson.toJson(new ResponseMessage("Done"));
            channel.basicPublish("", fileReponse, props, json.getBytes(StandardCharsets.UTF_8));
            publier(titreBoursier, OperationType.CREATE);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    private void publier(TitreBoursier titreBoursier, OperationType operationType) throws IOException {
        String json = gson.toJson(titreBoursier);
        AMQP.BasicProperties props = new AMQP.BasicProperties();
        HashMap map = new HashMap<String,Object>();
        map.put(titreBoursier.getMnemo(), "TRUE");
        map.put("OP", operationType.name());
        props = props.builder().headers(map).build();
        channel.basicPublish(EXCHANGE_NAME, "", props, json.getBytes(StandardCharsets.UTF_8));
    }

    public static void main(String[] argv) throws Exception {
        ServiceBourse service = new ServiceBourse();

        TitreBoursier google = new TitreBoursier("GOOG", "Google Inc.", 391.03f, 0.0f);
        TitreBoursier microsoft = new TitreBoursier("MSFT", "Microsoft Corp.", 25.79f, 0.0f);
        service.updateTitre(google);
        service.updateTitre(microsoft);

        service.publier(google, OperationType.CREATE);
        System.out.println("Publication de "+google);
        service.publier(microsoft, OperationType.CREATE);
        System.out.println("Publication de "+microsoft);
        Thread.sleep(10000);

        while(true) {
            service.publishAll();
            Thread.sleep(10000);
        }


    }

    private void publishAll() {
        for (TitreBoursier titreBoursier: titres.values()) {
            try {
                publier(titreBoursier, OperationType.UPDATE);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


}
