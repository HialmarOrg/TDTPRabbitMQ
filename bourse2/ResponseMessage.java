package bourse2;

/**
 * Réponses RPC
 */
public class ResponseMessage {
    private String status;

    /**
     * Constructeur
     * @param status statut de la réponse
     */
    public ResponseMessage(String status) {
        this.status = status;
    }

    /**
     * Constructeur par défaut
     */
    public ResponseMessage() {
    }

    /**
     * Renvoie le statut de la réponse
     * @return le statut de la réponse
     */
    public String getStatus() {
        return status;
    }

    /**
     * Précise le statut de la réponse
     * @param status le statut de la réponse
     */
    public void setStatus(String status) {
        this.status = status;
    }

    /**
     * Méthode d'affichage
     * @return une représentation textuelle
     */
    @Override
    public String toString() {
        return "ResponseMessage{" +
                "status='" + status + '\'' +
                '}';
    }
}
