package us.ligusan.base.tools.concurrent;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
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
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
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

    private final Set<T> runningTags;
    private final List<Pair<Runnable, T>> submittedTasks;
    private boolean shutdown;
    
    private final Lock lock;

    public TaggedThreadPoolExecutor(final int pQueueCapacity, final int pMaxNumberOfThreads, final long pKeepAliveTime, final TimeUnit pTimeUnit, final ThreadFactory pThreadFactory)
    {
        if((queueCapacity = pQueueCapacity) <= 0) throw new IllegalArgumentException();

        // san - Dec 9, 2018 4:01:52 PM : we maintain our own queue - no need to put extra
        executor = new ThreadPoolExecutor(1, pMaxNumberOfThreads, pKeepAliveTime, pTimeUnit, new FullBlockingQueue<>(), pThreadFactory);
        rejectionHandler = new Abort();

        runningTags = new HashSet<>(pMaxNumberOfThreads);
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
        ToStringBuilder lToStringBuilder =
            new ToStringBuilder(this).appendSuper(super.toString()).append("queueCapacity", queueCapacity).append("rejectionHandler", rejectionHandler).append("lock", lock);

        lock.lock();
        try
        {
            lToStringBuilder.append("executor", executor).append("runningTags", runningTags).append("submittedTasks", submittedTasks).append("shutdown", shutdown);
        }
        finally
        {
            lock.unlock();
        }

        return lToStringBuilder.toString();
    }

    @Override
    public void shutdown()
    {
        lock.lock();
        try
        {
            shutdown = true;
            // san - Dec 8, 2018 5:52:46 PM : submitted tasks should go through
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

            for(Pair<Runnable, T> lPair : submittedTasks)
                ret.add(lPair.getKey());
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
        lock.lock();
        try
        {
            return shutdown;
        }
        finally
        {
            lock.unlock();
        }
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
        return pTagged instanceof Tagged ? (T)((Tagged)pTagged).getTag() : null;
    }

    /*
     * Should be executed under lock.
     */
    protected boolean executeOrQueue(final Runnable pCommand)
    {
        T lTag = getTag(pCommand);
        if(!runningTags.contains(lTag) && runningTags.size() < executor.getMaximumPoolSize())
        {
            runningTags.add(lTag);

            executor.execute(() -> {
                try
                {
                    pCommand.run();
                }
                finally
                {
                    lock.lock();
                    try
                    {
                        runningTags.remove(lTag);

                        for(Iterator<Pair<Runnable, T>> lIterator = submittedTasks.iterator(); lIterator.hasNext();)
                        {
                            Pair<Runnable, T> lPair = lIterator.next();
                            if(!runningTags.contains(lPair.getValue()))
                            {
                                lIterator.remove();

                                // san - Dec 8, 2018 7:34:26 PM : bypassing shutdown check
                                executeOrQueue(lPair.getKey());

                                break;
                            }
                        }

                        // san - Dec 9, 2018 1:02:39 PM : all submitted, time to shutdown
                        if(shutdown && submittedTasks.isEmpty()) executor.shutdown();
                    }
                    finally
                    {
                        lock.unlock();
                    }
                }
            });
        }
        else if(submittedTasks.size() < queueCapacity) submittedTasks.add(new ImmutablePair<>(pCommand, lTag));
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
            if(!isShutdown()) throw new RejectedExecutionException(MessageFormat.format("Task {0} rejected from {1}", pRunnable, this));
        }
    }
    public class CallerRun implements Consumer<Runnable>
    {
        @Override
        public void accept(final Runnable pRunnable)
        {
            // san - Dec 9, 2018 3:07:07 PM : discard if shutting down
            if(!isShutdown()) pRunnable.run();
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
