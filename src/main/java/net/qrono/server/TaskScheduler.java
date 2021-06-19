package net.qrono.server;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

/**
 * An abstract task scheduler.
 */
public interface TaskScheduler {
  /**
   * Registers the given task with the scheduler. The returned handle can be used to schedule the
   * task for execution. The task will initially be unscheduled.
   */
  Handle register(Task task);

  /**
   * A task that runs periodically and cooperatively.
   */
  interface Task {
    /**
     * Runs the task for a single interval. Returns {@code true} if the tasks should be immediately
     * re-scheduled to run another interval. Otherwise, returns {@code false} to indicate the task
     * is idle.
     */
    boolean executeSingleInterval() throws IOException;
  }

  /**
   * Handle for a previously {@link #register(Task) registered} task allowing it to be scheduled or
   * canceled.
   */
  interface Handle {
    /**
     * Schedule the task for execution, if not already scheduled. If the task is currently running
     * then ensure it is re-scheduled after completing.
     */
    void schedule();

    /**
     * Cancels the task, preventing it from being scheduled or re-scheduled. If the task is
     * currently scheduled it will be unscheduled. If the task is currently running it will be
     * allowed to complete, but will not be permitted to be rescheduled.
     */
    CompletableFuture<Void> cancel();
  }
}
