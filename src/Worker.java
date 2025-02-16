public class Worker {
    private int id;
    private String name;
    private int capability;  // Worker skill level
    private int maxLoad;     // Max tasks the worker can handle at once
    private int assignedTasks;  // Number of tasks assigned to the worker

    // Constructor
    public Worker(int id, String name, int capability, int maxLoad) {
        this.id = id;
        this.name = name;
        this.capability = capability;
        this.maxLoad = maxLoad;
        this.assignedTasks = 0;  // Initially, no tasks assigned
    }

    // Getter and Setter methods
    public int getAssignedTasks() {
        return assignedTasks;
    }

    public void increaseAssignedTasks() {
        this.assignedTasks++;  // Increment assigned task count
    }

    public boolean canHandleMoreTasks() {
        return assignedTasks < maxLoad;  // Check if worker can handle more tasks
    }

    public int getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public int getCapability() {
        return capability;
    }

    public int getMaxLoad() {
        return maxLoad;
    }
}
