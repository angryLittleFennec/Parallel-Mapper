import ParallelMapper;

import java.util.*;
import java.util.function.Function;

public class ParallelMapperImpl implements ParallelMapper {
    private final List<Thread> workers;
    private final Queue<Runnable> tasks = new LinkedList<>();

    /**
     * Constructor from number of threads.
     *
     * @param threads number of threads.
     *
     */
    public ParallelMapperImpl(int threads) throws InterruptedException {
        workers = new ArrayList<>();
        InterruptedException exceptions = new InterruptedException();

        for (int i = 0; i < threads; i++) {
            workers.add(new Thread(() -> {
                try{
                    while (!Thread.interrupted()) {
                        Runnable task;
                        synchronized (tasks){
                            while (tasks.isEmpty()) {
                                tasks.wait();
                            }
                            task = tasks.poll();
                            tasks.notify();
                            }
                        task.run();
                    }
                } catch (InterruptedException e) {
                    exceptions.addSuppressed(e);
                }
            }));
            workers.get(i).start();
        }
        if (exceptions.getSuppressed().length != 0) {
            throw exceptions;
        }
    }

    private static class SynchronizedList<T> {
        private final List<T> list;
        private int size;
        RuntimeException e;

        SynchronizedList(int size) {
            this.list = new ArrayList<>();
            for (int i = 0; i < size; i++) {
                this.list.add(null);
            }
            this.e = null;
        }

        synchronized private void set(int i, T value) {
            list.set(i, value);
            size++;
            if (size == list.size()) {
                notify();
            }
        }

        synchronized void setException(RuntimeException e) {
            this.e = e;
        }

        synchronized private List<T> getResult() throws InterruptedException {
            while (size < list.size()) {
                wait();
            }

            if (e != null) {
                throw e;
            }

            return list;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T, R> List<R> map(Function<? super T, ? extends R> function, List<? extends T> list) throws InterruptedException {
        SynchronizedList<R> result = new SynchronizedList<>(list.size());
        for (int i = 0; i < list.size(); i++) {
            final int ii = i;
            synchronized (tasks) {
                try {
                    tasks.add(() -> result.set(ii, function.apply(list.get(ii))));
                }catch (RuntimeException e){
                    result.setException(e);
                }
                tasks.notify();
            }
        }

        return result.getResult();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        for (Thread thread : workers) {
            thread.interrupt();
        }
        for (Thread thread : workers) {
            try {
                thread.join();
            } catch (InterruptedException ignored) {
            }
        }
    }
}
