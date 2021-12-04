package us.ligusan.base.tools.concurrent;

import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;
import us.ligusan.base.tools.lang.Tagged;

/**
 * Tagged {@link Runnable} implementation.
 * 
 * @author Alexander Prishchepov
 */
public class TaggedFutureTask<V, T> extends FutureTask<V> implements Tagged<T>
{
    private T tag;

    public TaggedFutureTask(final Callable<V> pCallable, final T pTag)
    {
        super(pCallable);

        tag = pTag;
    }
    public TaggedFutureTask(final Runnable pRunnable, final V pResult, final T pTag)
    {
        super(pRunnable, pResult);

        tag = pTag;
    }

    @Override
    public T getTag()
    {
        return tag;
    }
}
