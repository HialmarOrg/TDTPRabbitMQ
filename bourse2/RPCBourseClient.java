package bourse2;

import bourse1.TitreBoursier;
import com.google.gson.Gson;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Scanner;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

public class RPCBourseClient implements AutoCloseable {

    private Connection connection;
    private Channel channel;
    private String requestQueueName = "bourse_rpc";

    private Gson gson = new Gson();

    public RPCBourseClient() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        connection = factory.newConnection();
        channel = connection.createChannel();
    }

    public static void main(String[] argv) throws IOException, TimeoutException, ExecutionException, InterruptedException {
        RPCBourseClient bourseClient = new RPCBourseClient();

        Scanner scanner = new Scanner(System.in);

        // Boucle principale du menu
        while (true) {

            // Affichage du menu
            System.out.println("------------------------------------------------");
            System.out.println("Menu des titres boursiers");
            System.out.println("------------------------------------------------");
            System.out.println("1. Créer un titre");
            System.out.println("2. Lire un titre");
            System.out.println("3. Mettre à jour un titre");
            System.out.println("4. Supprimer un titre");
            System.out.println("5. Quitter");
            System.out.print("Votre choix : ");

            // Récupération du choix de l'utilisateur
            int choix = scanner.nextInt();

            // Traitement du choix de l'utilisateur
            switch (choix) {

                case 1:
                    // Création d'un titre
                    System.out.print("Nom du titre : ");
                    String nom = scanner.next();
                    System.out.print("Mnémonique du titre : ");
                    String mnemo = scanner.next();
                    System.out.print("Prix du titre : ");
                    float prix = scanner.nextFloat();
                    TitreBoursier titreBoursier = new TitreBoursier(mnemo, nom, prix, 0.0f);
                    bourseClient.call(titreBoursier, OperationType.CREATE);
                    break;

                case 2:
                    // Lecture d'un titre
                    System.out.print("Mnémonique du titre : ");
                    mnemo = scanner.next();
                    titreBoursier = new TitreBoursier(mnemo,"", 0.0f, 0.0f);
                    bourseClient.call(titreBoursier, OperationType.REQUEST);
                    break;

                case 3:
                    // Mise à jour d'un titre
                    System.out.print("Nom du titre : ");
                    nom = scanner.next();
                    System.out.print("Mnémonique du titre : ");
                    mnemo = scanner.next();
                    System.out.print("Prix du titre : ");
                    prix = scanner.nextFloat();
                    titreBoursier = new TitreBoursier(mnemo, nom, prix, 0.0f);
                    bourseClient.call(titreBoursier, OperationType.UPDATE);
                    break;

                case 4:
                    // Suppression d'un titre
                    System.out.print("Mnémonique du titre : ");
                    mnemo = scanner.next();
                    titreBoursier = new TitreBoursier(mnemo,"", 0.0f, 0.0f);
                    bourseClient.call(titreBoursier, OperationType.DELETE);
                    break;

                case 5:
                    // Quitter
                    System.out.println("Fermeture du menu.");
                    bourseClient.close();
                    return;

                default:
                    System.out.println("Choix invalide.");
                    break;
            }
        }
    }

    public void call(TitreBoursier titreBoursier, OperationType operationType) throws IOException, InterruptedException, ExecutionException {
        final String corrId = UUID.randomUUID().toString();
        String json = gson.toJson(titreBoursier);
        String replyQueueName = channel.queueDeclare().getQueue();
        HashMap map = new HashMap<String,Object>();
        map.put(titreBoursier.getMnemo(), "TRUE");
        map.put("OP", operationType.name());
        AMQP.BasicProperties props = new AMQP.BasicProperties
                .Builder()
                .correlationId(corrId)
                .replyTo(replyQueueName)
                .headers(map)
                .build();

        channel.basicPublish("", requestQueueName, props, json.getBytes(StandardCharsets.UTF_8));

        final CompletableFuture<String> response = new CompletableFuture<>();

        String ctag = channel.basicConsume(replyQueueName, true, (consumerTag, delivery) -> {
            if (delivery.getProperties().getCorrelationId().equals(corrId)) {
                response.complete(new String(delivery.getBody(), "UTF-8"));
            }
        }, consumerTag -> {
        });

        String result = response.get();
        channel.basicCancel(ctag);
        System.out.println("Réponse : "+result);
    }

    public void close() throws IOException {
        connection.close();
    }
}
