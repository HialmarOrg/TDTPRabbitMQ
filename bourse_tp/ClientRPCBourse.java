package bourse_tp;

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

/**
 * Client pour les RPC de la bourse
 */
public class ClientRPCBourse implements AutoCloseable {

    // Connexion
    private final Connection connection;
    // Canal de communication
    private final Channel channel;
    // File pour les requêtes
    private final String requestQueueName = "bourse_rpc";
    // Convertisseur JSON
    private final Gson gson = new Gson();

    /**
     * Constructeur
     * @throws IOException : si pb de communication avec le broker
     * @throws TimeoutException : si expiration de tempo
     */
    public ClientRPCBourse() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        // Le broker est sur la machine locale
        factory.setHost("localhost");
        // connexion et canal
        connection = factory.newConnection();
        channel = connection.createChannel();
    }

    /**
     * Main avec un menu CRUD sur les Titres
     * @param argv arguments inutilisés
     * @throws IOException en cas de pb d'E/S
     * @throws TimeoutException en cas de timeout
     * @throws ExecutionException en cas de pb dans la tâche de récupération de la réponse
     * @throws InterruptedException inutilisé ici
     */
    public static void main(String[] argv) throws IOException, TimeoutException, ExecutionException, InterruptedException {
        ClientRPCBourse bourseClient = new ClientRPCBourse();

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
            scanner.nextLine();

            // Traitement du choix de l'utilisateur
            switch (choix) {

                case 1:
                    // Création d'un titre
                    System.out.print("Mnémonique du titre : ");
                    String mnemo = scanner.nextLine();
                    System.out.print("Nom du titre : ");
                    String nom = scanner.nextLine();
                    System.out.print("Valeur du titre : ");
                    float prix = Float.parseFloat(scanner.nextLine());
                    System.out.print("Unité de valeur du titre : ");
                    String unite = scanner.nextLine();
                    TitreBoursier titreBoursier = new TitreBoursier(mnemo, nom, prix, unite, 0.0f, 0.0f);
                    bourseClient.call(titreBoursier, OperationType.CREATE);
                    break;

                case 2:
                    // Lecture d'un titre
                    System.out.print("Mnémonique du titre : ");
                    mnemo = scanner.next();
                    titreBoursier = new TitreBoursier(mnemo,"", 0.0f, "", 0.0f, 0.0f);
                    bourseClient.call(titreBoursier, OperationType.REQUEST);
                    break;

                case 3:
                    // Mise à jour d'un titre
                    System.out.print("Mnémonique du titre : ");
                    mnemo = scanner.nextLine();
                    System.out.print("Nom du titre : ");
                    nom = scanner.nextLine();
                    System.out.print("Valeur du titre : ");
                    prix = Float.parseFloat(scanner.nextLine());
                    System.out.print("Unité de valeur du titre : ");
                    unite = scanner.nextLine();
                    titreBoursier = new TitreBoursier(mnemo, nom, prix, unite, 0.0f, 0.0f);
                    bourseClient.call(titreBoursier, OperationType.UPDATE);
                    break;

                case 4:
                    // Suppression d'un titre
                    System.out.print("Mnémonique du titre : ");
                    mnemo = scanner.next();
                    titreBoursier = new TitreBoursier(mnemo,"", 0.0f, "", 0.0f, 0.0f);
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

    /**
     * Invocation RPC
     * @param titreBoursier titre boursier
     * @param operationType opération
     * @throws IOException en cas de pb d'E/S
     * @throws InterruptedException inutilisé
     * @throws ExecutionException en cas de pb dans le callback
     */
    public void call(TitreBoursier titreBoursier, OperationType operationType) throws IOException, InterruptedException, ExecutionException {
        // génération aléatoire du correlation Id
        final String corrId = UUID.randomUUID().toString();
        // conversion en JSON
        String json = gson.toJson(titreBoursier);
        // création de la file pour la réponse
        String replyQueueName = channel.queueDeclare().getQueue();
        // Ajout du type d'opération
        HashMap<String, Object> map = new HashMap<>();
        map.put("OP", operationType.name());
        // Construction des propriétés du message
        AMQP.BasicProperties props = new AMQP.BasicProperties
                .Builder()
                .correlationId(corrId) // l'id de corrélation
                .replyTo(replyQueueName) // la file de réponse
                .headers(map) // les entêtes (dont le type d'opération)
                .build();

        // Emission de la requête
        channel.basicPublish("", requestQueueName, props, json.getBytes(StandardCharsets.UTF_8));

        // Future pour récupérer la réponse
        final CompletableFuture<String> response = new CompletableFuture<>();

        // Calback pour récupérer la réponse
        String ctag = channel.basicConsume(replyQueueName, true, (consumerTag, delivery) -> {
            // si on a bien la bonne réponse
            if (delivery.getProperties().getCorrelationId().equals(corrId)) {
                // on complète le CompletableFuture avec la réponse
                response.complete(new String(delivery.getBody(), StandardCharsets.UTF_8));
            }
        }, consumerTag -> {
        });

        // On attend puis récupère la réponse
        String result = response.get();
        // On désinscrit le callback
        channel.basicCancel(ctag);
        // On affiche la réponse
        System.out.println("Réponse : "+result);
    }

    /**
     * Pour fermer la connexion avec le broker
     * @throws IOException en cas de pb d'E/S
     */
    public void close() throws IOException {
        connection.close();
    }
}
