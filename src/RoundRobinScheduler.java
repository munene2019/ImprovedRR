import java.sql.*;
import java.util.*;

public class RoundRobinScheduler {
    private Queue<Worker> workerQueue;
    private List<Task> taskList;
    private String modelUsed;

    public RoundRobinScheduler(List<Worker> workers, List<Task> tasks, String modelUsed) {
        this.workerQueue = new LinkedList<>(workers);
        this.taskList = new ArrayList<>(tasks);
        this.modelUsed = modelUsed;
    }


    private int generateBatchId() {
        return (int) (System.currentTimeMillis() / 1000);  // Unix timestamp in seconds
    }

    public void assignTasks() {
        if ("Traditional RR".equalsIgnoreCase(modelUsed)) {
            assignTasksTraditional();
        } else {
            assignTasksImproved();
        }
    }

    // 🔹 Traditional Round Robin - Simple Cyclic Assignment
    private void assignTasksTraditional() {
        int taskIndex = 0;
        while (!taskList.isEmpty()) {
            Worker worker = workerQueue.poll();
            if (worker == null) break;

            if (taskIndex >= taskList.size()) {
                taskIndex = 0;  // Reset index if we reach the end
            }

            Task task = taskList.get(taskIndex);
            System.out.println("Assigning Task (Traditional RR) -> Worker: " + worker.getName() +
                    ", Task: " + task.getName());

            saveAssignmentToDB(worker, task, generateBatchId());
            taskList.remove(taskIndex);  // Remove task after assignment

            workerQueue.offer(worker); // Put worker back in queue
        }
    }

    // 🔹 Improved Round Robin - Assigns Based on Complexity & Priority
    private void assignTasksImproved() {
        while (!taskList.isEmpty()) {
            Worker worker = workerQueue.poll();
            if (worker == null) break;

            // Skip worker if they have reached max load
            if (!worker.canHandleMoreTasks()) {
                System.out.println(worker.getName() + " has reached their load limit of " + worker.getMaxLoad() + ". Skipping assignment.");
                workerQueue.offer(worker); // Put worker back in queue
                continue;  // Skip this worker and move on to the next
            }

            Task selectedTask = null;
            int maxPriority = Integer.MIN_VALUE;
            int closestComplexityDiff = Integer.MAX_VALUE;

            for (Task task : taskList) {
                int complexityDiff = Math.abs(task.getComplexity() - worker.getCapability());

                // ✅ Ensure only tasks with complexity ≤ worker capability are considered
                if (task.getComplexity() <= worker.getCapability()) {
                    // Prefer exact complexity matches with highest priority
                    if (task.getComplexity() == worker.getCapability() && task.getPriority() > maxPriority) {
                        maxPriority = task.getPriority();
                        selectedTask = task;
                    }
                    // Otherwise, assign the closest (but never higher) complexity task
                    else if (task.getComplexity() < worker.getCapability() && complexityDiff < closestComplexityDiff) {
                        closestComplexityDiff = complexityDiff;
                        selectedTask = task;
                    }
                }
            }

            if (selectedTask != null) {
                System.out.println("Assigning Task (Improved RR) -> Worker: " + worker.getName() +
                        ", Task: " + selectedTask.getName());

                saveAssignmentToDB(worker, selectedTask, generateBatchId());
                taskList.remove(selectedTask);
                worker.increaseAssignedTasks();  // Increment assigned tasks after assignment
            } else {
                System.out.println("No suitable task found for " + worker.getName());
            }

            workerQueue.offer(worker);
        }
    }

    private void saveAssignmentToDB(Worker worker, Task task, int batchId) {
        System.out.println("Saving to DB -> Worker: " + worker.getId() +
                ", Task: " + task.getId() +
                ", Model: " + modelUsed + ", Batch: " + batchId);

        try (Connection conn = new DB().getConnection();
             PreparedStatement stmt = conn.prepareStatement(
                     "INSERT INTO rr.assignments (worker_id, task_id, model_used, task_complexity, worker_skill_level, batch_id) VALUES (?, ?, ?, ?, ?, ?)")) {
            stmt.setInt(1, worker.getId());
            stmt.setInt(2, task.getId());
            stmt.setString(3, modelUsed);
            stmt.setInt(4, task.getComplexity());
            stmt.setInt(5, worker.getCapability());
            stmt.setInt(6, batchId);
            stmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    // 🔹 Database Connection


    public static void main(String[] args) {
        List<Worker> workers = fetchWorkersFromDB();
        List<Task> tasks = fetchTasksFromDB();

        if (workers.isEmpty() || tasks.isEmpty()) {
            System.out.println("No workers or tasks found. Exiting...");
            return;
        }

//        System.out.println("\n=== Assigning Tasks Using Improved RR ===");
//        RoundRobinScheduler improvedRR = new RoundRobinScheduler(workers, tasks, "Improved RR");
//        improvedRR.assignTasks();
     System.out.println("\n=== Assigning Tasks Using Traditional RR ===");
       RoundRobinScheduler traditionalRR = new RoundRobinScheduler(workers, tasks, "Traditional RR");
       traditionalRR.assignTasks();
    }

    // 🔹 Fetch Workers from DB
    public static List<Worker> fetchWorkersFromDB() {
        List<Worker> workers = new ArrayList<>();
        String sql = "SELECT worker_id, name, skill_level, load_limit FROM workers";  // Assuming 'max_load' is added to the database
        try (Connection conn = new DB().getConnection(); PreparedStatement stmt = conn.prepareStatement(sql); ResultSet rs = stmt.executeQuery()) {
            while (rs.next()) {
                workers.add(new Worker(rs.getInt("worker_id"), rs.getString("name"),
                        rs.getInt("skill_level"), rs.getInt("load_limit")));  // Fetch max_load from DB
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return workers;
    }

    // 🔹 Fetch Tasks from DB
    public static List<Task> fetchTasksFromDB() {
        List<Task> tasks = new ArrayList<>();
        String sql = "SELECT task_id, name, priority, complexity FROM tasks";
        try (Connection conn = new DB().getConnection(); PreparedStatement stmt = conn.prepareStatement(sql); ResultSet rs = stmt.executeQuery()) {
            while (rs.next()) {
                tasks.add(new Task(rs.getInt("task_id"), rs.getString("name"), rs.getInt("priority"), rs.getInt("complexity")));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return tasks;
    }
}
