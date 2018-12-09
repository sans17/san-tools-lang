package us.ligusan.base.tools.concurrent;

/**
 * Simple tagging interface.
 * 
 * @author Alexander Prishchepov
 */
public interface Tagged<T>
{
    default T getTag()
    {
        return null;
    }
}
