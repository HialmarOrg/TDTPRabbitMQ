package bourse_tp;

/**
 * La classe des titres boursiers
 */
public class TitreBoursier {
    private String mnemo;
    private String nomComplet;
    private float valeur;
    private String uniteValeur;
    private float variation;
    private float variationPourcent;

    /**
     * Constructeur par défaut
     */
    public TitreBoursier() {
    }

    /**
     * Constructeur avec tous les paramètres
     * @param mnemo : le mnémonique du titre boursier
     * @param nomComplet : le nom complet de l'entreprise
     * @param valeur : le cours actuel de l'action
     * @param uniteValeur : l'unité de la valeur
     * @param variation : la variation (calculée par le serveur)
     */
    public TitreBoursier(String mnemo, String nomComplet, float valeur, String uniteValeur, float variation, float variationPourcent) {
        this.mnemo = mnemo;
        this.nomComplet = nomComplet;
        this.valeur = valeur;
        this.uniteValeur = uniteValeur;
        this.variation = variation;
        this.variationPourcent = variationPourcent;
    }

    /**
     * Retourne le cours actuel de l'action
     * @return le cours actuel de l'action
     */
    public float getValeur() {
        return valeur;
    }

    /**
     * Retourne le mnémonique du titre boursier
     * @return le mnémonique du titre boursier
     */
    public String getMnemo() {
        return mnemo;
    }

    /**
     * Retourne le nom complet de l'entreprise
     * @return le nom complet de l'entreprise
     */
    public String getNomComplet() {
        return nomComplet;
    }

    /**
     * Retourne l'unité de la valeur
     * @return l'unité de la valeur
     */
    public String getUniteValeur() {
        return uniteValeur;
    }

    /**
     * Retourne la variation (calculée par le serveur)
     * @return la variation (calculée par le serveur)
     */
    public float getVariation() {
        return variation;
    }


    /**
     * Modifie la variation (calculée par le serveur)
     * @param variation : la variation (calculée par le serveur)
     */
    public void setVariation(float variation) {
        this.variation = variation;
    }

    /**
     * Retourne la variation en pourcent (calculée par le serveur)
     * @return la variation en pourcent (calculée par le serveur)
     */
    public float getVariationPourcent() {
        return variationPourcent;
    }

    /**
     * Modifie la variation en pourcent (calculée par le serveur)
     * @param variationPourcent : la variation en pourcent (calculée par le serveur)
     */
    public void setVariationPourcent(float variationPourcent) {
        this.variationPourcent = variationPourcent;
    }


    /**
     * Méthode d'affichage
     * @return une représentation textuelle
     */
    public String toString() {
        return "Titre : "+mnemo+" ("+nomComplet+") Cours : "+valeur+" "+uniteValeur+" Variation : "+variation+" "+variationPourcent+"%";
    }
}
