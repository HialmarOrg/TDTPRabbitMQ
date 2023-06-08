package ecom;

import bourse1.TitreBoursier;
import com.google.gson.Gson;
import com.rabbitmq.client.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class ServiceClient {

    private static final String QUEUE_ENCOURS = "ecom_encours";
    private static final String QUEUE_ANNULE = "ecom_annule";
    private static final String EXCHANGE_VALIDE = "ecom_valide";
    private Channel channel;
    private Gson gson = new Gson();

    Map<Integer, Commande> commandeMap = new HashMap<>();
    Map<Integer, Boolean> reponseStocks = new HashMap<>();
    Map<Integer, Boolean> reponseBanque = new HashMap<>();

    public ServiceClient() throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        channel = connection.createChannel();

        channel.queueDeclare(QUEUE_ENCOURS,false, false, false, null);

        channel.queueDeclare(QUEUE_ANNULE,false, false, false, null);

        channel.exchangeDeclare(EXCHANGE_VALIDE, "fanout");

        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            System.out.println(" [x] Received '" + message + "'");
            Commande commande = gson.fromJson(message, Commande.class);
            Object obj = delivery.getProperties().getHeaders().get("Emetteur");
            System.out.println(obj);
            traiter(commande, obj.toString());

        };
        channel.basicConsume(QUEUE_ENCOURS, true, deliverCallback, consumerTag -> { });
    }

    private void traiter(Commande commande, String emetteur) throws IOException {
        System.out.println("Traiter "+commande+" Emetteur "+emetteur);
        boolean fini = false;
        boolean valide = false;
        switch(emetteur) {
            case "Stocks":
                System.out.println("Reception réponse stocks pour commande "+commande.getId());
                reponseStocks.put(commande.getId(), true);
                if (reponseBanque.containsKey(commande.getId()) && reponseBanque.get(commande.getId())) {
                    fini = true;
                    commandeMap.get(commande.getId()).setStockOk(commande.isStockOk());
                    valide = commande.isStockOk() && commandeMap.get(commande.getId()).isBanqueOk();
                    System.out.println("Fini, valide ? "+valide+" "+commandeMap.get(commande.getId()));
                } else {
                    commandeMap.put(commande.getId(), commande);
                    reponseStocks.put(commande.getId(), true);
                    System.out.println("En cours "+commande);
                }
                break;
            case "Fact":
                System.out.println("Reception réponse banque pour commande "+commande.getId());
                reponseBanque.put(commande.getId(), true);
                if (reponseStocks.containsKey(commande.getId()) && reponseStocks.get(commande.getId())) {
                    fini = true;
                    commandeMap.get(commande.getId()).setBanqueOk(commande.isBanqueOk());
                    valide = commande.isBanqueOk() && commandeMap.get(commande.getId()).isStockOk();
                    System.out.println("Fini, valide ? "+valide+" "+commandeMap.get(commande.getId()));
                } else {
                    commandeMap.put(commande.getId(), commande);
                    reponseBanque.put(commande.getId(), true);
                    System.out.println("En cours "+commande);
                }
                break;
        }
        if(fini) {
            String json = gson.toJson(commandeMap.get(commande.getId()));
            if (valide)
                channel.basicPublish(EXCHANGE_VALIDE, "", null, json.getBytes(StandardCharsets.UTF_8));
            else
                channel.basicPublish("", QUEUE_ANNULE, null, json.getBytes(StandardCharsets.UTF_8));
        }
    }

    public static void main(String[] argv) throws Exception {
        new ServiceClient();
    }
}
