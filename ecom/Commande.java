package ecom;

public class Commande {
    private int id;
    private String produit;
    private String infoClient;
    private int quantite;
    private boolean stockOk;
    private boolean banqueOk;

    public Commande() {
    }

    public Commande(int id, String produit, String infoClient, int quantite) {
        this.id = id;
        this.produit = produit;
        this.infoClient = infoClient;
        this.quantite = quantite;
    }

    public int getId() {
        return id;
    }

    public String getProduit() {
        return produit;
    }

    public void setProduit(String produit) {
        this.produit = produit;
    }

    public String getInfoClient() {
        return infoClient;
    }

    public void setInfoClient(String infoClient) {
        this.infoClient = infoClient;
    }

    public int getQuantite() {
        return quantite;
    }

    public void setQuantite(int quantite) {
        this.quantite = quantite;
    }

    public boolean isStockOk() {
        return stockOk;
    }

    public void setStockOk(boolean stockOk) {
        this.stockOk = stockOk;
    }

    public boolean isBanqueOk() {
        return banqueOk;
    }

    public void setBanqueOk(boolean banqueOk) {
        this.banqueOk = banqueOk;
    }

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
