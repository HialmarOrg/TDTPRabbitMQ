package bourse_tp;

/**
 * La classe des titres boursiers
 */
public class TitreBoursier {
    private String mnemo;
    private String nomComplet;
    private float cours;
    private float variation;

    /**
     * Constructeur par défaut
     */
    public TitreBoursier() {
    }

    /**
     * Constructeur avec tous les paramètres
     * @param mnemo : le mnémonique du titre boursier
     * @param nomComplet : le nom complet de l'entreprise
     * @param cours : le cours actuel de l'action
     * @param variation : la variation (calculée par le serveur)
     */
    public TitreBoursier(String mnemo, String nomComplet, float cours, float variation) {
        this.mnemo = mnemo;
        this.nomComplet = nomComplet;
        this.cours = cours;
        this.variation = variation;
    }

    /**
     * Retourne le cours actuel de l'action
     * @return le cours actuel de l'action
     */
    public float getCours() {
        return cours;
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
     * Retourne la variation (calculée par le serveur)
     * @return la variation (calculée par le serveur)
     */
    public float getVariation() {
        return variation;
    }

    /**
     * Méthode d'affichage
     * @return une représentation textuelle
     */
    public String toString() {
        return "Titre : "+mnemo+" ("+nomComplet+") Cours : "+cours+" Variation : "+variation;
    }

    /**
     * Modifie la variation (calculée par le serveur)
     * @param variation : la variation (calculée par le serveur)
     */
    public void setVariation(float variation) {
        this.variation = variation;
    }

}
