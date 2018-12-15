package us.ligusan.base.tools.concurrent;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import java.util.Arrays;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

public class TaggedThreadPoolExecutorTest
{
    //    @BeforeAll
    //    static void setUpBeforeClass() throws Exception
    //    {}
    //    @AfterAll
    //    static void tearDownAfterClass() throws Exception
    //    {}
    //    @BeforeEach
    //    void setUp() throws Exception
    //    {}
    //    @AfterEach
    //    void tearDown() throws Exception
    //    {}

    @Test
    final void testExecuteDiscardOldest()
    {
        CopyOnWriteArrayList<Integer> lResultOfExecution = new CopyOnWriteArrayList<>();

        TaggedThreadPoolExecutor<Integer> lExecutorUnderTest = new TaggedThreadPoolExecutor<>(2, 2, 10, TimeUnit.SECONDS, Executors.defaultThreadFactory());
        lExecutorUnderTest.setRejectionHandler(lExecutorUnderTest.new DiscardOldest());

        for(int i = 0; i < 5; i++)
        {
            int j = i;
            lExecutorUnderTest.submit(new TaggedFutureTask<Object, Integer>(() -> lResultOfExecution.add(j), null, 1));
        }

        // san - Dec 13, 2018 8:48:05 PM : 0 goes into processing; 1, 2, 3, 4 into queue; 1, 2 are discarded 
        assertEquals(Arrays.asList(0, 3, 4), lResultOfExecution);
    }

    @Test
    final void testExecuteAbort()
    {
        CopyOnWriteArrayList<Integer> lResultOfExecution = new CopyOnWriteArrayList<>();

        TaggedThreadPoolExecutor<Integer> lExecutorUnderTest = new TaggedThreadPoolExecutor<>(2, 2, 10, TimeUnit.SECONDS, Executors.defaultThreadFactory());

        for(int i = 0; i < 5; i++)
        {
            int j = i;
            Executable lExecutable = () -> lExecutorUnderTest.submit(new TaggedFutureTask<Object, Integer>(() -> {
                // san - Dec 13, 2018 9:10:12 PM : 2 seconds sleep
                LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(2));
                lResultOfExecution.add(j);
            }, null, 1));

            if(i > 2) assertThrows(RejectedExecutionException.class, lExecutable);
            else try
            {
                lExecutable.execute();
            }
            catch(Throwable t)
            {
                fail(t);
            }
        }

        // san - Dec 13, 2018 9:12:18 PM : 10 seconds sleep
        LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(10));

        // san - Dec 13, 2018 8:48:05 PM : 0 goes into processing; 1, 2, 3, 4 into queue; 3, 4 aborted 
        assertEquals(Arrays.asList(0, 1, 2), lResultOfExecution);
    }
}
