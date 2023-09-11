package bourse2;

public class ResponseMessage {
    private String status;

    public ResponseMessage(String status) {
        this.status = status;
    }

    public ResponseMessage() {
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    @Override
    public String toString() {
        return "ResponseMessage{" +
                "status='" + status + '\'' +
                '}';
    }
}
