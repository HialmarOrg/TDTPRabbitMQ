package usine2;

import java.util.Date;

public class Piece {
    private boolean tansforme; // false = initial, true = transformé
    private Date dateCration;
    private Date dateModification;

    public Piece() {
        tansforme = false;
        dateModification = dateCration = new Date();
    }

    public boolean isTansforme() {
        return tansforme;
    }

    public void setTansforme(boolean tansforme) {
        this.tansforme = tansforme;
    }

    public Date getDateCration() {
        return dateCration;
    }

    public void setDateCration(Date dateCration) {
        this.dateCration = dateCration;
    }

    public Date getDateModification() {
        return dateModification;
    }

    public void setDateModification(Date dateModification) {
        this.dateModification = dateModification;
    }

    @Override
    public String toString() {
        return "Piece{" +
                "tansforme=" + tansforme +
                ", dateCration=" + dateCration +
                ", dateModification=" + dateModification +
                '}';
    }
}
