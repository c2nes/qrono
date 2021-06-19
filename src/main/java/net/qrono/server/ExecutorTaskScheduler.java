package net.qrono.server;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

public class ExecutorTaskScheduler implements TaskScheduler {
  private final Executor executor;

  public ExecutorTaskScheduler(Executor executor) {
    this.executor = executor;
  }

  @Override
  public Handle register(Task task) {
    return new TaskHandle(task);
  }

  private class TaskHandle implements Handle, Runnable {
    private final Task task;

    private TaskState state = TaskState.UNSCHEDULED;
    private boolean reschedule = false;
    private final CompletableFuture<Void> terminated = new CompletableFuture<>();

    private TaskHandle(Task task) {
      this.task = task;
    }

    private synchronized boolean isCanceled() {
      return state == TaskState.CANCELED;
    }

    @Override
    public synchronized void schedule() {
      // Check state
      if (isCanceled()) {
        try {
          terminated.getNow(null);
          throw new IllegalStateException("canceled");
        } catch (Exception e) {
          throw new IllegalStateException("canceled", e);
        }
      }

      switch (state) {
        case UNSCHEDULED:
          state = TaskState.SCHEDULED;
          reschedule = false;
          executor.execute(this);
          break;

        case SCHEDULED:
          // No-op. Already scheduled.
          break;

        case RUNNING:
          // Currently executing. Ensure the task reschedules itself, but do not submit
          // to executor as this would lead to multiple concurrent instances of the task.
          reschedule = true;
          break;
      }
    }

    @Override
    public synchronized CompletableFuture<Void> cancel() {
      // If the task is not actively running then we can jump straight to terminated.
      // Otherwise, we mark the task as canceled and leave it to run() to signal termination.
      if (state != TaskState.RUNNING) {
        terminated.complete(null);
      }

      state = TaskState.CANCELED;
      return terminated.copy();
    }

    @Override
    public void run() {
      synchronized (this) {
        if (isCanceled()) {
          return;
        }

        state = TaskState.RUNNING;
        reschedule = false;
      }

      try {
        boolean selfReschedule = task.executeSingleInterval();

        synchronized (this) {
          if (state == TaskState.CANCELED) {
            terminated.complete(null);
          } else if (selfReschedule || reschedule) {
            state = TaskState.SCHEDULED;
            executor.execute(this);
          } else {
            state = TaskState.UNSCHEDULED;
          }
        }
      } catch (Throwable e) {
        state = TaskState.CANCELED;
        terminated.completeExceptionally(e);

        if (e instanceof Error) {
          throw (Error) e;
        }
      }
    }
  }

  private enum TaskState {
    UNSCHEDULED,
    SCHEDULED,
    RUNNING,
    CANCELED,
  }
}
