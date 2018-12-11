package us.ligusan.base.tools.concurrent;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TaggedThreadPoolExecutorTest
{
    private TaggedThreadPoolExecutor<String> executorUnderTest;

    protected TaggedThreadPoolExecutor<String> getExecutorUnderTest()
    {
        return executorUnderTest;
    }

    protected void setExecutorUnderTest(final TaggedThreadPoolExecutor<String> pExecutorUnderTest)
    {
        executorUnderTest = pExecutorUnderTest;
    }

    //    @BeforeAll
    //    static void setUpBeforeClass() throws Exception
    //    {}
    //
    //    @AfterAll
    //    static void tearDownAfterClass() throws Exception
    //    {}

    @BeforeEach
    void setUp() throws Exception
    {
        TaggedThreadPoolExecutor<String> lExecutorUnderTest = new TaggedThreadPoolExecutor<>(2, 2, 10, TimeUnit.SECONDS, Executors.defaultThreadFactory());
        lExecutorUnderTest.setRejectionHandler(lExecutorUnderTest.new DiscardOldest());
        setExecutorUnderTest(lExecutorUnderTest);
    }

    //    @AfterEach
    //    void tearDown() throws Exception
    //    {}

    @Test
    final void testExecute()
    {
        Logger lLogger = System.getLogger(getClass().getName());

        TaggedThreadPoolExecutor<String> lExecutorUnderTest = getExecutorUnderTest();

        Future<?> lFuture = null;
        for(int i = 0; i < 5; i++)
        {
            lFuture = lExecutorUnderTest.submit(new TaggedFutureTask<String, String>(() -> {
                try
                {
                    Thread.currentThread().sleep(10_000);
                }
                catch(InterruptedException e)
                {
                    // TODO san - Dec 8, 2018 8:57:26 PM testExecute : what do we do here?
                }
                System.out.println("1");
            }, null, "1"));
            //            lLogger.log(Level.INFO, "executorUnderTest={0}", lExecutorUnderTest);
        }
        for(int i = 0; i < 5; i++)
        {
            lExecutorUnderTest.execute(() -> System.out.println("null"));
            //            lLogger.log(Level.INFO, "executorUnderTest={0}", lExecutorUnderTest);
        }

        lLogger.log(Level.INFO, "executorUnderTest={0}", lExecutorUnderTest);

        try
        {
            lFuture.get();
        }
        catch(Exception e)
        {
            Assertions.fail(e);
        }
    }
}
