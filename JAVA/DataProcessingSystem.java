import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class DataProcessingSystem {

    static class Task {
        final int id;
        final int value;

        Task(int id, int value) {
            this.id = id;
            this.value = value;
        }
    }

    static class SharedQueue {
        private final Queue<Task> queue = new ArrayDeque<>();

        public synchronized void addTask(Task task) {
            queue.add(task);
        }

        public synchronized Task getTask() {
            return queue.poll();
        }
    }

    static class Result {
        final int taskId;
        final int value;

        Result(int taskId, int value) {
            this.taskId = taskId;
            this.value = value;
        }

        public String toString() {
            return "Task " + taskId + " -> Result: " + value;
        }
    }

    static class ResultStore {
        private final List<Result> results = new ArrayList<>();

        public synchronized void addResult(Result result) {
            results.add(result);
        }

        public synchronized List<Result> snapshot() {
            return new ArrayList<>(results);
        }
    }

    static class Worker implements Runnable {
        private final int workerId;
        private final SharedQueue queue;
        private final ResultStore store;

        Worker(int workerId, SharedQueue queue, ResultStore store) {
            this.workerId = workerId;
            this.queue = queue;
            this.store = store;
        }

        @Override
        public void run() {
            System.out.println("Worker " + workerId + " started.");

            try {
                while (!Thread.currentThread().isInterrupted()) {
                    Task task = queue.getTask();

                    if (task == null) {
                        break;
                    }

                    try {
                        Thread.sleep(200);
                        int processed = task.value * task.value;

                        store.addResult(
                                new Result(task.id, processed));

                        System.out.println(
                                "Worker " + workerId +
                                        " processed Task " + task.id);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        System.err.println(
                                "Worker " + workerId +
                                        " interrupted during Task " + task.id);
                        break;
                    } catch (RuntimeException e) {
                        System.err.println(
                                "Worker " + workerId +
                                        " failed Task " + task.id +
                                        ": " + e.getMessage());
                    }
                }
            } finally {
                System.out.println(
                        "Worker " + workerId + " completed.");
            }
        }
    }

    public static void main(String[] args) {
        final int taskCount = 10;
        final int workerCount = 3;

        SharedQueue queue = new SharedQueue();
        ResultStore store = new ResultStore();

        for (int i = 1; i <= taskCount; i++) {
            queue.addTask(new Task(i, i * 10));
        }

        ExecutorService executor = Executors.newFixedThreadPool(workerCount);

        for (int i = 1; i <= workerCount; i++) {
            executor.submit(new Worker(i, queue, store));
        }

        executor.shutdown();

        try {
            if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                System.err.println("Workers timed out.");
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            System.err.println("Main thread interrupted.");
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        List<Result> results = store.snapshot();
        results.sort((a, b) -> Integer.compare(a.taskId, b.taskId));

        List<String> lines = new ArrayList<>();
        Set<Integer> uniqueIds = new HashSet<>();

        for (Result result : results) {
            lines.add(result.toString());
            uniqueIds.add(result.taskId);
        }

        try {
            Files.write(Path.of("java_results.txt"), lines);
            System.out.println("Results saved to java_results.txt");
        } catch (IOException e) {
            System.err.println("File error: " + e.getMessage());
        }

        for (String line : lines) {
            System.out.println(line);
        }

        System.out.println(
                "Processed " + results.size() +
                        " of " + taskCount + " tasks.");

        if (results.size() == taskCount &&
                uniqueIds.size() == taskCount) {
            System.out.println("All tasks processed exactly once.");
        } else {
            System.err.println(
                    "Warning: Missing or duplicate task results.");
        }
    }
}
