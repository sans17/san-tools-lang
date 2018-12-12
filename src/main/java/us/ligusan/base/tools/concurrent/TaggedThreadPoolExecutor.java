package us.ligusan.base.tools.concurrent;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
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
    
    private final Lock lock;

    public TaggedThreadPoolExecutor(final int pQueueCapacity, final int pMaxNumberOfThreads, final long pKeepAliveTime, final TimeUnit pTimeUnit, final ThreadFactory pThreadFactory)
    {
        if((queueCapacity = pQueueCapacity) <= 0) throw new IllegalArgumentException();

        // san - Dec 9, 2018 4:01:52 PM : we maintain our own queue - no need to put extra
        executor = new ThreadPoolExecutor(1, pMaxNumberOfThreads, pKeepAliveTime, pTimeUnit, new FullBlockingQueue<>(), pThreadFactory);
        rejectionHandler = new Abort();

        runningTasks = new ArrayList<>();
        submittedTasks = new ArrayList<>();
        
        lock = new ReentrantLock();
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
        ToStringBuilder lToStringBuilder = new ToStringBuilder(this).appendSuper(super.toString()).append("queueCapacity", queueCapacity).append("executor", executor)
            .append("rejectionHandler", rejectionHandler).append("shutdown", shutdown).append("lock", lock);

        lock.lock();
        try
        {
            lToStringBuilder.append("runningTasks", runningTasks).append("submittedTasks", submittedTasks);
        }
        finally
        {
            lock.unlock();
        }

        return lToStringBuilder.toString();
    }

    /*
     * Should be executed under lock.
     */
    protected void tryShutdown()
    {
        // san - Dec 10, 2018 2:11:24 PM : nothing is running - shutdown executor
        if(runningTasks.isEmpty()) executor.shutdown();
    }

    @Override
    public void shutdown()
    {
        lock.lock();
        try
        {
            shutdown = true;
            // san - Dec 8, 2018 5:52:46 PM : submitted tasks should go through

            tryShutdown();
        }
        finally
        {
            lock.unlock();
        }
    }
    @Override
    public List<Runnable> shutdownNow()
    {
        lock.lock();
        try
        {
            shutdown = true;

            ArrayList<Runnable> ret = new ArrayList<Runnable>(executor.shutdownNow());

            ret.addAll(submittedTasks);
            submittedTasks.clear();

            return ret;
        }
        finally
        {
            lock.unlock();
        }
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

    /*
     * Should be executed under lock.
     */
    protected boolean isRunningTag(final Runnable pRunnable)
    {
        T lTag = getTag(pRunnable);

        if(lTag != null) for(Runnable lRunnable : runningTasks)
            if(lTag.equals(getTag(lRunnable))) return true;

        return false;
    }

    /*
     * Should be executed under lock.
     */
    protected boolean executeOrQueue(final Runnable pRunnable)
    {
        // san - Dec 10, 2018 2:22:33 PM : execute
        // san - Dec 10, 2018 2:33:47 PM : null tag processed at any time
        if(!isRunningTag(pRunnable) && runningTasks.size() < executor.getMaximumPoolSize())
        {
            runningTasks.add(pRunnable);

            executor.execute(() -> {
                for(Runnable lRunnableToExecute = pRunnable; lRunnableToExecute != null;)
                    try
                    {
                        lRunnableToExecute.run();
                    }
                    catch(Throwable t)
                    {
                        // san - Dec 11, 2018 7:10:00 PM : yep - I just swallowed a Throwable from a Runnable
                    }
                    finally
                    {
                        lock.lock();
                        try
                        {
                            runningTasks.remove(lRunnableToExecute);

                            lRunnableToExecute = null;

                            for(Iterator<Runnable> lIterator = submittedTasks.iterator(); lIterator.hasNext();)
                            {
                                Runnable lQueuedRunnable = lIterator.next();
                                if(!isRunningTag(lQueuedRunnable))
                                {
                                    // san - Dec 8, 2018 7:34:26 PM : will be running in the same thread
                                    runningTasks.add(lRunnableToExecute = lQueuedRunnable);

                                    lIterator.remove();

                                    break;
                                }
                            }

                            // san - Dec 9, 2018 1:02:39 PM : check if it is time to shutdown
                            if(shutdown) tryShutdown();
                        }
                        finally
                        {
                            lock.unlock();
                        }
                    }
            });
        }
        // san - Dec 10, 2018 2:22:50 PM : or queue
        else if(submittedTasks.size() < queueCapacity) submittedTasks.add(pRunnable);
        // san - Dec 10, 2018 2:21:55 PM : will need to reject
        else return false;

        return true;
    }
    @Override
    public void execute(final Runnable pCommand)
    {
        boolean lAdded = true;

        lock.lock();
        try
        {
            if(!shutdown) lAdded = executeOrQueue(pCommand);
            // san - Dec 8, 2018 8:01:07 PM : discard if shutting down
        }
        finally
        {
            lock.unlock();
        }

        // san - Dec 8, 2018 7:50:26 PM : special handling if queue is full
        // san - Dec 9, 2018 9:33:26 PM : lock is released here, so it is possible that execution order will be different from submission order with some handlers. I guess, it is the same with ThreadPoolExecutor 
        if(!lAdded) rejectionHandler.accept(pCommand);
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
            // san - Dec 9, 2018 3:07:07 PM : discard if shutting down
            if(!shutdown) throw new RejectedExecutionException(MessageFormat.format("Task {0} rejected from {1}", pRunnable, this));
        }
    }
    public class CallerRun implements Consumer<Runnable>
    {
        @Override
        public void accept(final Runnable pRunnable)
        {
            // san - Dec 9, 2018 3:07:07 PM : discard if shutting down
            if(!shutdown) pRunnable.run();
        }
    }
    public class DiscardOldest implements Consumer<Runnable>
    {
        @Override
        public void accept(final Runnable pRunnable)
        {
            lock.lock();
            try
            {
                // san - Dec 9, 2018 3:07:07 PM : discard if shutting down
                if(!shutdown)
                {
                    // san - Dec 9, 2018 3:04:01 PM : remove oldest
                    if(submittedTasks.size() >= queueCapacity) submittedTasks.remove(0);

                    // san - Dec 9, 2018 3:04:08 PM : retry insertion
                    executeOrQueue(pRunnable);
                }
            }
            finally
            {
                lock.unlock();
            }
        }
    }
    public static class Discard implements Consumer<Runnable>
    {
        @Override
        public void accept(final Runnable pRunnable)
        {}
    }
}
