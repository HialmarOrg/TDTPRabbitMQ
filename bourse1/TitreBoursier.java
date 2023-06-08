package bourse1;

public class TitreBoursier {
    private String mnemo;
    private String nomComplet;
    private float cours;
    private float variation;

    public TitreBoursier(String mnemo, String nomComplet, float cours, float variation) {
        this.mnemo = mnemo;
        this.nomComplet = nomComplet;
        this.cours = cours;
        this.variation = variation;
    }

    public float getCours() {
        return cours;
    }

    public String getMnemo() {
        return mnemo;
    }

    public String getNomComplet() {
        return nomComplet;
    }

    public float getVariation() {
        return variation;
    }

    public String toString() {
        return "Titre : "+mnemo+" ("+nomComplet+") Cours : "+cours+" Variation : "+variation;
    }

    public void setVariation(float variation) {
        this.variation = variation;
        cours += variation;
    }

}
