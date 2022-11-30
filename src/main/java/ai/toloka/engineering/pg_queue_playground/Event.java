package ai.toloka.engineering.pg_queue_playground;

public class Event {

    public final Long id;
    public final String payload;

    public Event(String payload) {
        this(null, payload);
    }

    public Event(Long id, String payload) {
        this.id = id;
        this.payload = payload;
    }

    @Override
    public String toString() {
        return "Event{" +
                "id=" + id +
                '}';
    }
}
