package ecom;

/**
 * DTO représentant une commande
 */
public class Commande {
    private int id;
    private String produit; // nom du produit
    private String infoClient; // infos bancaires
    private int quantite;
    private boolean stockOk; // est-ce que les stocks sont ok (valide après passage par les stocks)
    private boolean banqueOk; // est-ce que la banque est ok (valide après passage par la banque)

    /**
     * Constructeur par défaut (ne pas oublier)
     */
    public Commande() {
    }

    /**
     * Constructeur
     * @param id id de la commande
     * @param produit nom du produit
     * @param infoClient infos bancaires
     * @param quantite quantité de produit commandée
     */
    public Commande(int id, String produit, String infoClient, int quantite) {
        this.id = id;
        this.produit = produit;
        this.infoClient = infoClient;
        this.quantite = quantite;
    }

    /**
     * Getter de l'id
     * @return l'id
     */
    public int getId() {
        return id;
    }

    /**
     * Getter du produit
     * @return le nom du produit
     */
    public String getProduit() {
        return produit;
    }

    /**
     * Setter du produit
     * @param produit nouvean nom
     */
    public void setProduit(String produit) {
        this.produit = produit;
    }

    /**
     * Getter infos bancaires
     * @return infos bancaires
     */
    public String getInfoClient() {
        return infoClient;
    }

    /**
     * Setter infos bancaires
     * @param infoClient infos bancaires
     */
    public void setInfoClient(String infoClient) {
        this.infoClient = infoClient;
    }

    /**
     * Getter quantité
     * @return quantité
     */
    public int getQuantite() {
        return quantite;
    }

    /**
     * Setter quantité
     * @param quantite quantité
     */
    public void setQuantite(int quantite) {
        this.quantite = quantite;
    }

    /**
     * Les stocks sont-ils ok ? (uniquement valide après passage par les stocks)
     * @return stocks ok ?
     */
    public boolean isStockOk() {
        return stockOk;
    }

    /**
     * Permet de préciser si les stocks sont ok
     * @param stockOk true si ok
     */
    public void setStockOk(boolean stockOk) {
        this.stockOk = stockOk;
    }

    /**
     * Les infos bancaires sont-elles ok ? (uniquement valide après passage par la banque)
     * @return banque ok ?
     */
    public boolean isBanqueOk() {
        return banqueOk;
    }

    /**
     * Permet de préciser si les infos bancaires sont ok
     * @param banqueOk true si ok
     */
    public void setBanqueOk(boolean banqueOk) {
        this.banqueOk = banqueOk;
    }

    /**
     * Méthode pour l'édition
     * @return une chaine
     */
    @Override
    public String toString() {
        return "Commande{" +
                "id=" + id +
                ", produit='" + produit + '\'' +
                ", infoClient='" + infoClient + '\'' +
                ", quantite=" + quantite +
                ", stockOk=" + stockOk +
                ", banqueOk=" + banqueOk +
                '}';
    }
}
