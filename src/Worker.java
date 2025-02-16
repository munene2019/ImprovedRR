public class Worker {
    private int id;
    private String name;
    private int capability;

    public Worker(int id, String name, int capability) {
        this.id = id;
        this.name = name;
        this.capability = capability;
    }

    public int getId() { return id; }
    public String getName() { return name; }
    public int getCapability() { return capability; }
}
