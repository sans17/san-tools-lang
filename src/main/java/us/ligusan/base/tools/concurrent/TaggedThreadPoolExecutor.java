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

    protected void tryShutdown()
    {
        boolean lShutdown = true;

        lock.lock();
        try
        {
            lShutdown = runningTasks.isEmpty();
        }
        finally
        {
            lock.unlock();
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

        lock.lock();
        try
        {
            ret = new ArrayList<Runnable>(submittedTasks);
            submittedTasks.clear();
        }
        finally
        {
            lock.unlock();
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
        lock.lock();
        try
        {
            if(runningTasks.size() >= executor.getMaximumPoolSize()) return false;

            T lTag = getTag(pRunnable);
            // san - Dec 10, 2018 2:33:47 PM : null tag processed at any time
            if(lTag != null) for(Runnable lRunnable : runningTasks)
                if(lTag.equals(getTag(lRunnable))) return false;

            runningTasks.add(pRunnable);
        }
        finally
        {
            lock.unlock();
        }

        return true;
    }

    @Override
    public void execute(final Runnable pCommand)
    {
        // san - Dec 8, 2018 8:01:07 PM : discard if shutting down
        if(!shutdown)
            // san - Dec 10, 2018 2:22:33 PM : execute
            if(addToRunning(pCommand)) executor.execute(() -> {
                for(Runnable lRunnableToExecute = pCommand; lRunnableToExecute != null;)
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
                                if(addToRunning(lQueuedRunnable))
                                {
                                    // san - Dec 8, 2018 7:34:26 PM : will be running in the same thread
                                    lRunnableToExecute = lQueuedRunnable;

                                    lIterator.remove();

                                    break;
                                }
                            }
                        }
                        finally
                        {
                            lock.unlock();
                        }

                        // san - Dec 9, 2018 1:02:39 PM : check if it is time to shutdown
                        if(shutdown) tryShutdown();
                    }
            });
            else
        {
            boolean lQueued = false;

            lock.lock();
            try
            {
                // san - Dec 10, 2018 2:22:50 PM : or queue
                if(lQueued = submittedTasks.size() < queueCapacity) submittedTasks.add(pCommand);
            }
            finally
            {
                lock.unlock();
            }

            // san - Dec 8, 2018 7:50:26 PM : special handling if queue is full
            if(!lQueued) rejectionHandler.accept(pCommand);
        }
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
            lock.lock();
            try
            {
                // san - Dec 9, 2018 3:04:01 PM : remove oldest
                if(submittedTasks.size() >= queueCapacity) submittedTasks.remove(0);

                // san - Dec 9, 2018 3:04:08 PM : retry under lock so that nobody takes our place in queue
                execute(pRunnable);
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
