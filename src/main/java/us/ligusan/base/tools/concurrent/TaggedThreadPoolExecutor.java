package us.ligusan.base.tools.concurrent;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import us.ligusan.base.tools.collections.FullBlockingQueue;
import us.ligusan.base.tools.lang.Tagged;

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

    private final Semaphore executorSemaphore;
    // san - Dec 11, 2018 8:11:04 PM : we can identify thread by number, in case we have the same runnable twice
    private final Map<Integer, Runnable> runningTasks;
    private final BlockingQueue<Runnable> submittedTasks;
    private volatile boolean shutdown;

    public TaggedThreadPoolExecutor(final int pQueueCapacity, final int pMaxNumberOfThreads, final long pKeepAliveTime, final TimeUnit pTimeUnit, final ThreadFactory pThreadFactory)
    {
        // san - Dec 9, 2018 4:01:52 PM : we maintain our own queue - no need to for extra queueing
        // san - Dec 14, 2018 11:07:05 PM : 1 core thread, since FullBlockingQueue always rejects, ThreadPoolExecutor will add threads up to max 
        executor = new ThreadPoolExecutor(1, pMaxNumberOfThreads, pKeepAliveTime, pTimeUnit, new FullBlockingQueue<>(), pThreadFactory, new RejectedExecutionHandler()
            {
                @Override
                public void rejectedExecution(final Runnable pRunnable, final ThreadPoolExecutor pExecutor)
                {
                    // san - Dec 19, 2018 8:51:03 PM : we add threads with very strict control - semaphore and map of runnables. The only way we can be rejected, if old thread has not exited yet. Let give it another chance with yield. 
                    Thread.yield();

                    // san - Dec 19, 2018 9:16:26 PM : yes, it is a recursive call, and we can get SOE, but I don't see any other way
                    pExecutor.execute(pRunnable);
                }
            });
        // san - Dec 14, 2018 11:24:51 PM : default is to abort
        rejectionHandler = new Abort();

        executorSemaphore = new Semaphore(pMaxNumberOfThreads);
        runningTasks = new ConcurrentHashMap<>(pMaxNumberOfThreads);
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

    protected void tryShutdown()
    {
        // san - Dec 10, 2018 2:11:24 PM : nothing is running - shutdown executor
        if(runningTasks.isEmpty()) executor.shutdown();
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
        synchronized(submittedTasks)
        {
            submittedTasks.drainTo(ret);
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

    protected boolean isTagCollision(final int pThreadNumber)
    {
        T lTag = getTag(runningTasks.get(pThreadNumber));
        // san - Dec 10, 2018 2:33:47 PM : null tag processed at any time
        if(lTag != null) for(Entry<Integer, Runnable> lEntry : runningTasks.entrySet())
            // san - Dec 15, 2018 2:59:57 PM : found same tag
            // san - Dec 15, 2018 9:21:53 PM : will need to remove
            if(pThreadNumber != lEntry.getKey() && lTag.equals(getTag(lEntry.getValue()))) return true;
        // san - Dec 16, 2018 4:25:36 PM : no tags collision
        return false;
    }

    protected int addToRunning(final Runnable pRunnable)
    {
        // san - Dec 15, 2018 2:51:07 PM : let's try to add our runnable first
        for(int i = 1; i <= executor.getMaximumPoolSize(); i++)
            if(runningTasks.putIfAbsent(i, pRunnable) == null) return i;

        // san - Dec 16, 2018 4:18:04 PM : no thread available
        return 0;
    }

    protected int dequeue(final int pThreadNumber)
    {
        int ret = pThreadNumber;

        // san - Dec 17, 2018 9:02:20 PM : it's ok if current tag is null
        T lCurrentTag = getTag(runningTasks.get(ret));

        synchronized(submittedTasks)
        {
            for(Iterator<Runnable> lIterator = submittedTasks.iterator(); lIterator.hasNext();)
            {
                Runnable lRunnable = lIterator.next();

                // san - Dec 16, 2018 4:49:19 PM : already added - need to change Runnable
                if(ret > 0) runningTasks.put(ret, lRunnable);
                // san - Dec 16, 2018 4:41:42 PM : let's check if we can add anything in
                else
                // san - Dec 16, 2018 4:42:38 PM : could not add - no need to check the rest
                if((ret = addToRunning(lRunnable)) < 1) return 0;

                // san - Dec 17, 2018 8:59:55 PM : if we have the same tag or there is no tag collision
                // san - Dec 14, 2018 10:22:49 PM : next runnable is ok to continue
                if(Objects.equals(lCurrentTag, getTag(lRunnable)) || !isTagCollision(ret))
                {
                    lIterator.remove();

                    return ret;
                }
            }
        }

        // san - Dec 16, 2018 4:11:32 PM : there was a tag collision - need to remove
        runningTasks.remove(ret);
        return 0;
    }

    protected boolean isQueueEmpty()
    {
        synchronized(submittedTasks)
        {
            return submittedTasks.isEmpty();
        }
    }

    protected void passToExecutor(final int pThreadNumber)
    {
        Runnable lRunnable = () -> {
            int lThreadNumber = pThreadNumber;
            boolean lInterrupted = false;
            do
            {
                // san - Dec 17, 2018 9:35:50 PM : this can happen on retry
                if(lThreadNumber < 1) lThreadNumber = dequeue(0);

                // san - Dec 19, 2018 9:01:41 PM : check if we are interrupted, clearing flag
                // san - Dec 8, 2018 7:34:26 PM : will be running in the same thread
                for(; !(lInterrupted = Thread.interrupted()) && lThreadNumber > 0; lThreadNumber = dequeue(lThreadNumber))
                    try
                    {
                        runningTasks.get(lThreadNumber).run();
                    }
                    catch(Throwable t)
                    {
                        // san - Dec 11, 2018 7:10:00 PM : yep - I just swallowed a Throwable from a Runnable
                    }
            }
            // san - Dec 15, 2018 3:10:40 PM : unintentional tags collision, let's try again
            while(!lInterrupted && runningTasks.isEmpty() && !isQueueEmpty());

            // san - Dec 19, 2018 9:13:10 PM : just in case we were interrupted
            runningTasks.remove(lThreadNumber);

            // san - Dec 9, 2018 1:02:39 PM : check if it is time to shutdown
            if(shutdown) tryShutdown();

            // san - Dec 19, 2018 8:40:38 PM : this thread is done
            executorSemaphore.release();
        };

        for(boolean lAdded = false; !lAdded;)
            // san - Jan 26, 2019 3:12:44 PM : if worker ended processing, let's just add another task
            if(lAdded = executor.getPoolSize() < executor.getMaximumPoolSize()) executor.execute(lRunnable);
            // san - Jan 26, 2019 3:14:47 PM : otherwise let's wait in queue - 10 millis?
            else try
            {
                // san - Jan 26, 2019 3:32:47 PM : waited offer, so that FullBlockingQueue will wait
                lAdded = executor.getQueue().offer(lRunnable, 10, TimeUnit.MILLISECONDS);
            }
            catch(InterruptedException e)
            {
                // san - Jan 26, 2019 3:31:24 PM : interrupted? who cares
                Thread.interrupted();
            }
    }

    protected void tryRunning()
    {
        // san - Dec 19, 2018 8:46:53 PM : adding new thread
        if(executorSemaphore.tryAcquire())
        {
            int lThreadNumber = dequeue(0);
            if(lThreadNumber > 0) passToExecutor(lThreadNumber);
            // san - Dec 19, 2018 8:47:02 PM : did not work - removing
            else executorSemaphore.release();
        }
    }

    protected boolean enqueue(final Runnable pRunnable)
    {
        synchronized(submittedTasks)
        {
            return submittedTasks.offer(pRunnable);
        }
    }

    @Override
    public void execute(final Runnable pCommand)
    {
        if(pCommand == null) throw new NullPointerException();

        // san - Dec 8, 2018 8:01:07 PM : discard if shutting down
        if(!shutdown)
        {
            // san - Dec 15, 2018 8:58:41 PM : 1-based; 0 - not found
            int lThreadNumber = 0;
            do
                // san - Dec 19, 2018 8:39:57 PM : let's try to add a thread
                // san - Dec 15, 2018 3:04:28 PM : there is an open thread
                // san - Dec 16, 2018 4:26:57 PM : but we have tags collision
                if(executorSemaphore.tryAcquire() && (lThreadNumber = addToRunning(pCommand)) > 0 && isTagCollision(lThreadNumber))
                {
                    runningTasks.remove(lThreadNumber);

                    lThreadNumber = 0;

                    // san - Dec 19, 2018 8:41:52 PM : was not able to add to running tasks
                    executorSemaphore.release();
                }
            // san - Dec 15, 2018 3:10:40 PM : something is removing threads while we adding - let's try again
            while(lThreadNumber < 1 && runningTasks.isEmpty());

            // san - Dec 10, 2018 2:22:33 PM : execute
            if(lThreadNumber > 0) passToExecutor(lThreadNumber);
            // san - Dec 10, 2018 2:22:50 PM : or queue
            else if(enqueue(pCommand)) tryRunning();
            // san - Dec 8, 2018 7:50:26 PM : special handling if queue is full
            else rejectionHandler.accept(pCommand);
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
            // san - Dec 14, 2018 4:56:25 PM : try to insert
            while(!enqueue(pRunnable))
                // san - Dec 14, 2018 4:56:37 PM : no luck - remove first
                synchronized(submittedTasks)
                {
                    submittedTasks.poll();
                }

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
