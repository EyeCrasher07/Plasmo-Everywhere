package dev.eyecrasher.everywhere.core;

import java.net.*;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

/** Bounded TCP round-trip sampling. Runs only on the connection worker, never on a game thread. */
public final class NodeSelection {
    private static final long CACHE_NANOS=TimeUnit.SECONDS.toNanos(60),FAILED_NANOS=TimeUnit.SECONDS.toNanos(30);
    private record Sample(long measuredAt,long nanos) {}
    private record Choice(NetworkDirectory.Node node,long nanos,int index) {}
    @FunctionalInterface interface Probe { long measure(NetworkDirectory.Node node,long deadline) throws Exception; }
    private static final Map<URI,Sample> cache=new LinkedHashMap<>();
    private static final Map<URI,Long> failures=new LinkedHashMap<>();
    private static final ThreadPoolExecutor WORKERS=new ThreadPoolExecutor(4,4,0,TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<>(32),r -> { Thread t=new Thread(r,"everywhere-node-probe");t.setDaemon(true);return t; });
    private NodeSelection() {}
    public static NetworkDirectory.Route select(NetworkDirectory.Route route,ConnectionTrace trace) throws InterruptedException {
        return select(route,trace,NodeSelection::probe,4000);
    }
    static NetworkDirectory.Route select(NetworkDirectory.Route route,ConnectionTrace trace,Probe probe,long budgetMillis) throws InterruptedException {
        if(route.nodes().isEmpty())return route;
        long started=System.nanoTime(),deadline=started+TimeUnit.MILLISECONDS.toNanos(budgetMillis);
        int attempt=trace.attempts();trace.recordForAttempt(attempt,"NODE_SELECTION","START",0,null,route.nodes().size());
        CompletionService<Choice> results=new ExecutorCompletionService<>(WORKERS);List<Future<Choice>> pending=new ArrayList<>();
        List<Choice> choices=new ArrayList<>();
        try {
            for(int n=0;n<route.nodes().size();n++) {
                var node=route.nodes().get(n);int index=n;
                try { pending.add(results.submit(() -> {
                    synchronized(cache) {
                        Long failed=failures.get(node.control());
                        if(failed!=null && System.nanoTime()-failed<FAILED_NANOS)return null;
                        Sample saved=cache.get(node.control());
                        if(saved!=null && System.nanoTime()-saved.measuredAt<CACHE_NANOS)return new Choice(node,saved.nanos,index);
                    }
                    long value;
                    try { value=probe.measure(node,deadline); }
                    catch(Exception unavailable) { trace.recordForAttempt(attempt,"NODE_PROBE","FAILED",-1,unavailable,index);return null; }
                    if(value<0 || System.nanoTime()>deadline)return null;
                    synchronized(cache) {
                        if(cache.size()>=32)cache.remove(cache.keySet().iterator().next());cache.put(node.control(),new Sample(System.nanoTime(),value));
                    }
                    trace.recordForAttempt(attempt,"NODE_PROBE","SUCCESS",TimeUnit.NANOSECONDS.toMillis(value),null,index);
                    return new Choice(node,value,index);
                })); } catch(RejectedExecutionException busy) { break; }
            }
            for(int n=0;n<pending.size();n++) {
                long remaining=deadline-System.nanoTime();if(remaining<=0)break;
                Future<Choice> completed=results.poll(remaining,TimeUnit.NANOSECONDS);if(completed==null)break;
                try { Choice choice=completed.get();if(choice!=null)choices.add(choice); }
                catch(ExecutionException ignored) {}
            }
        } finally { pending.forEach(value -> value.cancel(true)); }
        if(Thread.currentThread().isInterrupted())throw new InterruptedException();
        Choice best=choices.stream().min(Comparator.comparingLong(Choice::nanos).thenComparing(value -> value.node.id())).orElse(null);
        trace.recordForAttempt(attempt,"NODE_SELECTION",best==null?"FALLBACK":"SUCCESS",
                TimeUnit.NANOSECONDS.toMillis(System.nanoTime()-started),null,best==null?-1:best.index);
        if(best==null)return route;
        LinkedHashMap<URI,NetworkDirectory.Probe> discovery=new LinkedHashMap<>();
        discovery.put(best.node.control(),new NetworkDirectory.Probe(best.node.control(),best.node.udpPort()));
        for(var node:route.nodes())if(discovery.size()<4)discovery.putIfAbsent(node.control(),new NetworkDirectory.Probe(node.control(),node.udpPort()));
        return new NetworkDirectory.Route(best.node.control(),List.copyOf(discovery.values()),route.nodes());
    }
    public static void failed(URI node) {
        synchronized(cache) {
            cache.remove(node);if(failures.size()>=32)failures.remove(failures.keySet().iterator().next());failures.put(node,System.nanoTime());
        }
    }
    private static long probe(NetworkDirectory.Node node,long deadline) throws Exception {
        // DNS is deliberately outside the measured connect time, but covered by the selection deadline.
        InetAddress[] addresses=InetAddress.getAllByName(node.control().getHost());
        long[] samples=new long[3];int count=0;Exception failure=null;
        for(InetAddress address:addresses) {
            for(int n=0;n<3;n++) {
                long remaining=deadline-System.nanoTime();if(remaining<=0 || Thread.currentThread().isInterrupted())throw new InterruptedException();
                try(Socket socket=new Socket()) {
                    long started=System.nanoTime();socket.connect(new InetSocketAddress(address,node.control().getPort()>0?node.control().getPort():443),
                            (int)Math.max(1,Math.min(900,TimeUnit.NANOSECONDS.toMillis(remaining))));
                    samples[count++]=System.nanoTime()-started;
                } catch(IOException unavailable) { failure=unavailable;break; }
                if(count==3) { Arrays.sort(samples);return samples[1]; }
            }
            count=0;
        }
        throw failure==null?new IOException("No reachable node address"):failure;
    }
}
