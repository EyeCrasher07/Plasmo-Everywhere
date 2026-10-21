package dev.eyecrasher.everywhere.core;

import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

public final class NodeSelectionTest {
    private static NetworkDirectory.Node node(String id) { id=id.substring(0,Math.min(id.length(),32));return new NetworkDirectory.Node(id,URI.create("wss://"+id+".invalid/voice"),24454); }
    private static void check(boolean value,String label) { if(!value)throw new AssertionError(label); }
    public static void verify() throws Exception {
        NetworkDirectory.Node slow=node("slow"+UUID.randomUUID()),fast=node("fast"+UUID.randomUUID()),dead=node("dead"+UUID.randomUUID());
        var route=new NetworkDirectory.Route(slow.control(),List.of(),List.of(slow,fast,dead));ConnectionTrace trace=new ConnectionTrace();trace.startAttempt();
        Thread caller=Thread.currentThread();AtomicInteger probes=new AtomicInteger();
        NodeSelection.Probe sampler=(node,deadline) -> {
            check(Thread.currentThread()!=caller,"Latency probe blocked the calling thread");probes.incrementAndGet();
            if(node.equals(dead))throw new ConnectException();return TimeUnit.MILLISECONDS.toNanos(node.equals(fast)?10:90);
        };
        var selected=NodeSelection.select(route,trace,sampler,1000);
        check(selected.control().equals(fast.control()),"Lowest median RTT was not selected");
        check(selected.probes().get(0).control().equals(fast.control()),"Selected relay not retained for traversal");
        check(probes.get()==3,"Reachable and unavailable nodes were not sampled");
        var cached=new NetworkDirectory.Route(slow.control(),List.of(),List.of(slow,fast));
        check(NodeSelection.select(cached,trace,(node,deadline) -> { throw new AssertionError("Cached RTT was re-probed"); },1000).control().equals(fast.control()),"Cached RTT choice lost");
        NodeSelection.failed(fast.control());
        check(NodeSelection.select(cached,trace,sampler,1000).control().equals(slow.control()),"Failed node cooldown ignored");
        var legacy=new NetworkDirectory.Route(slow.control(),List.of());
        check(NodeSelection.select(legacy,trace,(node,deadline) -> { throw new AssertionError(); },1000)==legacy,"Legacy affinity changed without a federation capability");
        var timeout=new NetworkDirectory.Route(slow.control(),List.of(),List.of(node("stall"+UUID.randomUUID())));
        long started=System.nanoTime();
        check(NodeSelection.select(timeout,trace,(node,deadline) -> { Thread.sleep(10000);return 0; },80)==timeout,"Timeout did not preserve safe fallback");
        check(System.nanoTime()-started<TimeUnit.SECONDS.toNanos(2),"Node selection exceeded its deadline");
        CountDownLatch entered=new CountDownLatch(1);AtomicBoolean interrupted=new AtomicBoolean();
        Thread cancellation=new Thread(() -> {
            try { NodeSelection.select(timeout,trace,(node,deadline) -> { entered.countDown();Thread.sleep(10000);return 0; },4000); }
            catch(InterruptedException expected) { interrupted.set(true); }
        });
        cancellation.start();check(entered.await(2,TimeUnit.SECONDS),"Cancellation probe did not start");cancellation.interrupt();cancellation.join(2000);
        check(!cancellation.isAlive() && interrupted.get(),"Cancelled connection retained its selection worker");
        try(ServerSocket socket=new ServerSocket(0,16,InetAddress.getLoopbackAddress())) {
            var local=new NetworkDirectory.Node("local",URI.create("ws://127.0.0.1:"+socket.getLocalPort()+"/voice"),24454);
            var localRoute=new NetworkDirectory.Route(slow.control(),List.of(),List.of(local));
            check(NodeSelection.select(localRoute,trace).control().equals(local.control()),"Real TCP endpoint not selected");
        }
        System.out.println("Node selection passed: RTT ranking, cache, failure cooldown, legacy fallback, deadline, cancellation and real TCP probes");
    }
}
