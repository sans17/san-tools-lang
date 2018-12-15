package us.ligusan.base.tools.concurrent;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.commons.lang3.builder.ToStringBuilder;
import us.ligusan.base.tools.collections.FullBlockingQueue;

/**
 * Implementation of <a href="https://www.javaspecialists.eu/archive/Issue206.html">Striped Executor</a> with a single queue.<br>
 * 
 * It implements standard ExecutorService interface, maintains single queue, takes a maximum number of threads as a parameter, support different rejection policies (similar to standard
 * {@link ThreadPoolExecutor}).
 * Unlike {@link ThreadPoolExecutor} it starts new thread not when queue is full, but when new task is submitted.
 * 
 * @author Alexander Prishchepov
 */
public class TaggedThreadPoolExecutor<T> extends AbstractExecutorService
{
    private final ThreadPoolExecutor executor;
    private volatile Consumer<Runnable> rejectionHandler;

    // san - Dec 11, 2018 8:11:04 PM : use list in case we have the same runnable twice
    private final List<Runnable> runningTasks;
    private final BlockingQueue<Runnable> submittedTasks;
    private volatile boolean shutdown;
    
    public TaggedThreadPoolExecutor(final int pQueueCapacity, final int pMaxNumberOfThreads, final long pKeepAliveTime, final TimeUnit pTimeUnit, final ThreadFactory pThreadFactory)
    {
        // san - Dec 9, 2018 4:01:52 PM : we maintain our own queue - no need to for extra queueing
        // san - Dec 14, 2018 11:07:05 PM : 1 core thread, since FullBlockingQueue always rejects, ThreadPoolExecutor will add threads up to max 
        executor = new ThreadPoolExecutor(1, pMaxNumberOfThreads, pKeepAliveTime, pTimeUnit, new FullBlockingQueue<>(), pThreadFactory);
        // san - Dec 14, 2018 11:24:51 PM : default is to abort
        rejectionHandler = new Abort();

        runningTasks = new ArrayList<>();
        // san - Dec 13, 2018 7:21:33 PM : will be removing a lot from 0 and middle - LinkedQueue?
        // san - Dec 14, 2018 4:44:42 PM : with LinkedBlockingQueue there is not need for synchronization or capacity check
        submittedTasks = new LinkedBlockingQueue<>(pQueueCapacity);
    }

    public Consumer<Runnable> getRejectionHandler()
    {
        return rejectionHandler;
    }
    public void setRejectionHandler(final Consumer<Runnable> pRejectionHandler)
    {
        // san - Dec 14, 2018 11:28:18 PM : ThreadPoolExecutor throws NPE in this case
        if(pRejectionHandler == null) throw new NullPointerException();

        rejectionHandler = pRejectionHandler;
    }
    @Override
    public String toString()
    {
        ToStringBuilder lToStringBuilder =
            new ToStringBuilder(this).appendSuper(super.toString()).append("executor", executor).append("rejectionHandler", rejectionHandler);
        synchronized(runningTasks)
        {
            lToStringBuilder.append("runningTasks", runningTasks);
        }
        return lToStringBuilder.append("submittedTasks", submittedTasks).append("shutdown", shutdown).toString();
    }

    protected boolean isDone()
    {
        synchronized(runningTasks)
        {
            return runningTasks.isEmpty();
        }
    }

    protected void tryShutdown()
    {
        // san - Dec 10, 2018 2:11:24 PM : nothing is running - shutdown executor
        if(isDone()) executor.shutdown();
    }

    @Override
    public void shutdown()
    {
        shutdown = true;
        // san - Dec 8, 2018 5:52:46 PM : submitted tasks should go through

        tryShutdown();
    }
    @Override
    public List<Runnable> shutdownNow()
    {
        shutdown = true;

        ArrayList<Runnable> ret = new ArrayList<>();
        submittedTasks.drainTo(ret);

        executor.shutdownNow();

        return ret;
    }
    @Override
    public boolean isShutdown()
    {
        return shutdown;
    }
    @Override
    public boolean isTerminated()
    {
        return executor.isTerminated();
    }
    @Override
    public boolean awaitTermination(final long pTimeout, final TimeUnit pUnit) throws InterruptedException
    {
        return executor.awaitTermination(pTimeout, pUnit);
    }

    protected boolean isFull()
    {
        synchronized(runningTasks)
        {
            return runningTasks.size() >= executor.getMaximumPoolSize();
        }
    }

    protected T getTag(final Object pTagged)
    {
        // san - Dec 9, 2018 9:37:58 PM : it does not really matter what T is - jvm will strip it out
        return pTagged instanceof Tagged ? (T)((Tagged)pTagged).getTag() : null;
    }

    protected boolean addToRunning(final Runnable pRunnableToStart)
    {
        if(isFull()) return false;

        T lTag = getTag(pRunnableToStart);
        synchronized(runningTasks)
        {
            // san - Dec 10, 2018 2:33:47 PM : null tag processed at any time
            if(lTag != null) for(Runnable lRunnable : runningTasks)
                // san - Dec 14, 2018 10:37:35 PM : if we have the same runnable twice, they should have the same tag
                if(lTag.equals(getTag(lRunnable))) return false;

            // san - Dec 14, 2018 10:38:35 PM : adding Runnable to list of threads
            return runningTasks.add(pRunnableToStart);
        }
    }

    protected Runnable nextRunning(final Runnable pCurrentlyRunning)
    {
        // san - Dec 15, 2018 11:46:35 AM : with simple synchronization we have to make sure that what we enter into running is the same that what we pull from queue
        synchronized(runningTasks)
        {
            if(pCurrentlyRunning != null) runningTasks.remove(pCurrentlyRunning);

            for(Iterator<Runnable> lIterator = submittedTasks.iterator(); lIterator.hasNext();)
            {
                Runnable ret = lIterator.next();
                // san - Dec 14, 2018 10:22:49 PM : trying to add new runnable
                if(addToRunning(ret))
                {
                    lIterator.remove();

                    return ret;
                }
            }

            // san - Dec 14, 2018 10:22:16 PM : remove currently running - it is already done
        }

        return null;
    }

    protected void passToExecutor(final Runnable pRunnable)
    {
        if(pRunnable != null) executor.execute(() -> {
            // san - Dec 8, 2018 7:34:26 PM : will be running in the same thread
            for(Runnable lRunnable = pRunnable; lRunnable != null; lRunnable = nextRunning(lRunnable))
                try
                {
                    lRunnable.run();
                }
                catch(Throwable t)
                {
                    // san - Dec 11, 2018 7:10:00 PM : yep - I just swallowed a Throwable from a Runnable
                }

            // san - Dec 9, 2018 1:02:39 PM : check if it is time to shutdown
            if(shutdown) tryShutdown();
        });
    }

    protected void tryRunning()
    {
        // san - Dec 12, 2018 9:25:07 PM : it is possible that thread finished while we were queueing
        passToExecutor(nextRunning(null));
    }

    @Override
    public void execute(final Runnable pCommand)
    {
        if(pCommand == null) throw new NullPointerException();

        // san - Dec 8, 2018 8:01:07 PM : discard if shutting down
        if(!shutdown)
            // san - Dec 10, 2018 2:22:33 PM : execute
            if(addToRunning(pCommand)) passToExecutor(pCommand);
            // san - Dec 10, 2018 2:22:50 PM : or queue
            else if(submittedTasks.offer(pCommand)) tryRunning();
            // san - Dec 8, 2018 7:50:26 PM : special handling if queue is full
            else rejectionHandler.accept(pCommand);
    }

    @Override
    protected <V> RunnableFuture<V> newTaskFor(final Runnable pRunnable, final V pValue)
    {
        return new TaggedFutureTask<V, T>(pRunnable, pValue, getTag(pRunnable));
    }
    @Override
    protected <V> RunnableFuture<V> newTaskFor(final Callable<V> pCallable)
    {
        return new TaggedFutureTask<V, T>(pCallable, getTag(pCallable));
    }

    public class Abort implements Consumer<Runnable>
    {
        @Override
        public void accept(final Runnable pRunnable)
        {
            throw new RejectedExecutionException(MessageFormat.format("Task {0} rejected from {1}", pRunnable, TaggedThreadPoolExecutor.this));
        }
    }
    public static class CallerRun implements Consumer<Runnable>
    {
        @Override
        public void accept(final Runnable pRunnable)
        {
            pRunnable.run();
        }
    }
    public class DiscardOldest implements Consumer<Runnable>
    {
        @Override
        public void accept(final Runnable pRunnable)
        {
            // san - Dec 14, 2018 4:56:25 PM : try to insert
            // san - Dec 14, 2018 4:56:37 PM : no luck - remove first
            while(!submittedTasks.offer(pRunnable))
                submittedTasks.poll();

            tryRunning();
        }
    }
    public static class Discard implements Consumer<Runnable>
    {
        @Override
        public void accept(final Runnable pRunnable)
        {}
    }
}
