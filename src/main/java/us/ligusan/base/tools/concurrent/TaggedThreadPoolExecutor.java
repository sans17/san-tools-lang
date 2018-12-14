package us.ligusan.base.tools.concurrent;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.commons.lang3.builder.ToStringBuilder;
import us.ligusan.base.tools.collections.FullBlockingQueue;

/**
 * Implementation of Striped Executor
 * https://www.javaspecialists.eu/archive/Issue206.html
 * with a single queue.
 * 
 * @author Alexander Prishchepov
 */
public class TaggedThreadPoolExecutor<T> extends AbstractExecutorService
{
    private final int queueCapacity;

    private final ThreadPoolExecutor executor;
    private volatile Consumer<Runnable> rejectionHandler;

    // san - Dec 11, 2018 8:11:04 PM : use list in case we have the same runnable twice
    private final List<Runnable> runningTasks;
    private final List<Runnable> submittedTasks;
    private volatile boolean shutdown;
    
    public TaggedThreadPoolExecutor(final int pQueueCapacity, final int pMaxNumberOfThreads, final long pKeepAliveTime, final TimeUnit pTimeUnit, final ThreadFactory pThreadFactory)
    {
        if((queueCapacity = pQueueCapacity) <= 0) throw new IllegalArgumentException();

        // san - Dec 9, 2018 4:01:52 PM : we maintain our own queue - no need to put extra
        executor = new ThreadPoolExecutor(1, pMaxNumberOfThreads, pKeepAliveTime, pTimeUnit, new FullBlockingQueue<>(), pThreadFactory);
        rejectionHandler = new Abort();

        runningTasks = new ArrayList<>();
        // san - Dec 13, 2018 7:21:33 PM : will be removing a lot from 0 and middle - LinkedList would be better?
        submittedTasks = new LinkedList<>();
    }

    public Consumer<Runnable> getRejectionHandler()
    {
        return rejectionHandler;
    }
    public void setRejectionHandler(final Consumer<Runnable> pRejectionHandler)
    {
        rejectionHandler = pRejectionHandler;
    }
    @Override
    public String toString()
    {
        ToStringBuilder lToStringBuilder =
            new ToStringBuilder(this).appendSuper(super.toString()).append("queueCapacity", queueCapacity).append("executor", executor).append("rejectionHandler", rejectionHandler);
        synchronized(runningTasks)
        {
            lToStringBuilder.append("runningTasks", runningTasks);
        }
        synchronized(submittedTasks)
        {
            lToStringBuilder.append("submittedTasks", submittedTasks);
        }

        return lToStringBuilder.append("shutdown", shutdown).toString();
    }

    protected void tryShutdown()
    {
        boolean lShutdown = true;

        synchronized(runningTasks)
        {
            lShutdown = runningTasks.isEmpty();
        }

        // san - Dec 10, 2018 2:11:24 PM : nothing is running - shutdown executor
        if(lShutdown) executor.shutdown();
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

        ArrayList<Runnable> ret = null;
        synchronized(submittedTasks)
        {
            ret = new ArrayList<>(submittedTasks);
            submittedTasks.clear();
        }

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

    protected T getTag(final Object pTagged)
    {
        // san - Dec 9, 2018 9:37:58 PM : it does not really matter what T is - jvm will strip it out
        return pTagged instanceof Tagged ? (T)((Tagged)pTagged).getTag() : null;
    }

    protected boolean addToRunning(final Runnable pRunnable)
    {
        synchronized(runningTasks)
        {
            if(runningTasks.size() >= executor.getMaximumPoolSize()) return false;

            T lTag = getTag(pRunnable);
            // san - Dec 10, 2018 2:33:47 PM : null tag processed at any time
            if(lTag != null) for(Runnable lRunnable : runningTasks)
                if(lTag.equals(getTag(lRunnable))) return false;

            return runningTasks.add(pRunnable);
        }
    }

    protected Runnable nextRunning(final Runnable pRunnable)
    {
        // san - Dec 13, 2018 7:25:50 PM : this method requires both locks
        synchronized(runningTasks)
        {
            // san - Dec 13, 2018 7:19:04 PM : if pRunnable is not null, we execute in pooled thread - addToRunning should happen in the same lock
            if(pRunnable != null) runningTasks.remove(pRunnable);

            synchronized(submittedTasks)
            {
                for(Iterator<Runnable> lIterator = submittedTasks.iterator(); lIterator.hasNext();)
                {
                    Runnable ret = lIterator.next();
                    if(addToRunning(ret))
                    {
                        lIterator.remove();

                        return ret;
                    }
                }
            }
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

    protected boolean enqueue(final Runnable pRunnable)
    {
        synchronized(submittedTasks)
        {
            return submittedTasks.size() < queueCapacity && submittedTasks.add(pRunnable);
        }
    }

    protected boolean isDone()
    {
        synchronized(runningTasks)
        {
            return runningTasks.isEmpty();
        }
    }

    protected void tryRunning()
    {
        // san - Dec 12, 2018 9:25:07 PM : it is possible that all running tasks are done
        if(isDone()) passToExecutor(nextRunning(null));
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
            else if(enqueue(pCommand)) tryRunning();
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

    protected void discardOldest(final Runnable pRunnable)
    {
        synchronized(submittedTasks)
        {
            // san - Dec 9, 2018 3:04:01 PM : remove oldest
            if(submittedTasks.size() >= queueCapacity) submittedTasks.remove(0);
            submittedTasks.add(pRunnable);
        }
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
            discardOldest(pRunnable);

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
