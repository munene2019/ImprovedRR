public class Task {
    private int id;
    private String name;
    private int complexity;
    private int priority;

    public Task(int id, String name, int complexity, int priority) {
        this.id = id;
        this.name = name;
        this.complexity = complexity;
        this.priority = priority;
    }

    public int getId() { return id; }
    public String getName() { return name; }
    public int getComplexity() { return complexity; }
    public int getPriority() { return priority; }
}
