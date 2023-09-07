package bourse2;

import bourse1.TitreBoursier;
import com.google.gson.Gson;
import com.rabbitmq.client.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;

public class ServiceBourse {

    private static final String EXCHANGE_NAME = "bourse_headers";
    private Channel channel;
    private Gson gson = new Gson();

    public ServiceBourse() throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        channel = connection.createChannel();
        channel.exchangeDeclare(EXCHANGE_NAME, "bourse_headers");

        String queueName = channel.queueDeclare().getQueue();
        channel.queueBind(queueName, EXCHANGE_NAME, "", null);

        System.out.println(" Service Bourse [*] Waiting for messages. To exit press CTRL+C");

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            System.out.println(" Service Bourse [x] Received '" + message + "'");
        };
        channel.basicConsume(queueName, true, deliverCallback, consumerTag -> { });

        channel.queueDeclare("bourse_rpc", true, false, false, null);

        DeliverCallback deliverCallbackRPC = (consumerTag, delivery) -> {
            this.gestionRPC(consumerTag, delivery);
        };
        channel.basicConsume(queueName, true, deliverCallback, consumerTag -> { });


    }

    private void gestionRPC(String consumerTag, Delivery delivery) {
        Object op = delivery.getProperties().getHeaders().get("OP");
        if (op instanceof String) {
            OperationType operationType = OperationType.valueOf((String)op);
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            System.out.println(" [x] Received '" + message + "'");
            Gson gson = new Gson();
            TitreBoursier titreBoursier = gson.fromJson(message, TitreBoursier.class);
            switch(operationType){
                case CREATE -> this.createTitre(titreBoursier);
                case UPDATE -> this.updateTite(titreBoursier);
                case DELETE -> this.deleteTitre(titreBoursier);
                case REQUEST -> this.getTitre(titreBoursier);
            }
        }
    }

    private void getTitre(TitreBoursier titreBoursier) {
    }

    private void deleteTitre(TitreBoursier titreBoursier) {
    }

    private void updateTite(TitreBoursier titreBoursier) {
    }

    private void createTitre(TitreBoursier body) {
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

        service.publier(google, OperationType.CREATE);
        System.out.println("Publication de "+google);
        service.publier(microsoft, OperationType.CREATE);
        System.out.println("Publication de "+microsoft);
        Thread.sleep(1000);
        for(int i=0; i<10; i++) {
            float variation = (float)Math.random() * 20.0f - 10.0f;
            google.setVariation(variation);
            service.publier(google, OperationType.UPDATE);
            System.out.println("Publication de "+google);
            variation = (float)Math.random() * 20.0f - 10.0f;
            microsoft.setVariation(variation);
            service.publier(microsoft, OperationType.UPDATE);
            System.out.println("Publication de "+microsoft);
            Thread.sleep(1000);
        }
    }
}
