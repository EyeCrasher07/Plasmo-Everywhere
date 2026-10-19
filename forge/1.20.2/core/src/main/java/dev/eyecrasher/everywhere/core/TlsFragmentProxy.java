package dev.eyecrasher.everywhere.core;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/** A private, single-destination CONNECT adapter. TLS still terminates in the JDK client. */
final class TlsFragmentProxy implements AutoCloseable {
    private final String host, authority;
    private final int port;
    private final ServerSocket listener = new ServerSocket();
    private final Set<Socket> sockets = ConcurrentHashMap.newKeySet();
    private final Semaphore slots = new Semaphore(4);
    // One accept loop and at most four pairs of blocking pumps, with no task backlog.
    private final ExecutorService tasks = RuntimeCompat.workers("everywhere-tls-proxy", 9);
    private final AtomicBoolean closed = new AtomicBoolean();

    TlsFragmentProxy(URI target) throws IOException {
        if (!Set.of("https", "wss").contains(target.getScheme()) || target.getHost() == null
                || target.getUserInfo() != null) throw new IllegalArgumentException("TLS target required");
        String name = target.getHost();
        host = name.startsWith("[") ? name.substring(1, name.length()-1) : name;
        port = target.getPort() == -1 ? 443 : target.getPort();
        if (port < 1 || port > 65535) throw new IllegalArgumentException("TLS target port");
        authority = (host.contains(":") ? "["+host+"]" : host)+":"+port;
        try {
            listener.bind(new InetSocketAddress("127.0.0.1", 0), 4);
            tasks.execute(this::accept);
        } catch (IOException | RuntimeException error) { close(); throw error; }
    }
    InetSocketAddress address() { return (InetSocketAddress)listener.getLocalSocketAddress(); }
    private void accept() {
        while (!closed.get()) {
            try {
                Socket socket = listener.accept();
                if (!slots.tryAcquire()) { socket.close(); continue; }
                sockets.add(socket);
                try { tasks.execute(() -> serve(socket)); }
                catch (RejectedExecutionException error) { sockets.remove(socket); socket.close(); slots.release(); }
            } catch (IOException error) { if (!closed.get()) close(); return; }
        }
    }
    private void serve(Socket client) {
        Socket upstream = new Socket(); sockets.add(upstream);
        try (client; upstream) {
            if (closed.get()) return;
            client.setSoTimeout(8000); client.setTcpNoDelay(true);
            InputStream input = client.getInputStream(); OutputStream output = client.getOutputStream();
            String header = readHeader(input);
            String[] lines = header.split("\r\n");
            // Never use a request-supplied destination or expose a general-purpose local proxy.
            if (!lines[0].equalsIgnoreCase("CONNECT "+authority+" HTTP/1.1")) {
                output.write("HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".getBytes(StandardCharsets.US_ASCII));
                return;
            }
            upstream.connect(new InetSocketAddress(host, port), 8000); upstream.setTcpNoDelay(true);
            output.write("HTTP/1.1 200 Connection Established\r\n\r\n".getBytes(StandardCharsets.US_ASCII)); output.flush();
            // Bound the first record; nothing is decrypted and no certificate is substituted.
            byte[] first = readRecord(input);
            upstream.getOutputStream().write(fragment(first)); upstream.getOutputStream().flush();
            client.setSoTimeout(0);
            Future<?> returning = tasks.submit(() -> pump(upstream, client));
            try { pump(client, upstream); }
            finally { closeSocket(upstream); closeSocket(client); returning.cancel(true); }
        } catch (IOException | RuntimeException ignored) {
            // Do not log TLS bytes, request headers, addresses or authentication material.
        } finally { sockets.remove(client); sockets.remove(upstream); slots.release(); }
    }
    private static void pump(Socket source, Socket target) {
        try { source.getInputStream().transferTo(target.getOutputStream()); }
        catch (IOException ignored) {}
        finally { closeSocket(source); closeSocket(target); }
    }
    private static String readHeader(InputStream input) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream(); int match = 0;
        byte[] end = {'\r','\n','\r','\n'};
        while (match < 4) {
            int next = input.read();
            if (next < 0 || bytes.size() >= 4096) throw new IOException("CONNECT header limit");
            bytes.write(next); match = next == end[match] ? match+1 : next == '\r' ? 1 : 0;
        }
        return bytes.toString(StandardCharsets.US_ASCII);
    }
    static byte[] readRecord(InputStream input) throws IOException {
        byte[] header = input.readNBytes(5);
        if (header.length != 5 || header[0] != 22 || header[1] != 3) throw new IOException("ClientHello required");
        int size = u16(header, 3);
        if (size < 4 || size > 16384) throw new IOException("ClientHello record limit");
        byte[] body = input.readNBytes(size);
        if (body.length != size || body[0] != 1) throw new IOException("ClientHello required");
        ByteArrayOutputStream record = new ByteArrayOutputStream(5+size); record.writeBytes(header); record.writeBytes(body);
        return record.toByteArray();
    }
    /** Splits a plaintext ClientHello record, preserving the handshake byte stream exactly. */
    static byte[] fragment(byte[] record) throws IOException {
        if (record.length < 9 || record[0] != 22 || record[1] != 3 || record[5] != 1
                || u16(record, 3) != record.length-5 || record.length > 16389) throw new IOException("Invalid ClientHello record");
        int split = sniMiddle(record); // Within SNI when present; IP-literal endpoints have no SNI.
        ByteArrayOutputStream result = new ByteArrayOutputStream(record.length+5);
        writePart(result, record, 5, split-5); writePart(result, record, split, record.length-split);
        return result.toByteArray();
    }
    private static void writePart(ByteArrayOutputStream output, byte[] record, int start, int size) {
        output.write(record[0]); output.write(record[1]); output.write(record[2]);
        output.write(size >>> 8); output.write(size); output.write(record, start, size);
    }
    private static int sniMiddle(byte[] b) {
        // ClientHello may itself span records. A one-byte first fragment also works without parsing it.
        try {
            int handshake = ((b[6]&255)<<16) | u16(b, 7);
            if (handshake != b.length-9) return 6;
            int p = 43; p += 1+(b[p]&255); // legacy_version, random, session id
            p += 2+u16(b, p); p += 1+(b[p]&255); // cipher suites, compression
            int end = p+2+u16(b, p); p += 2;
            if (end != b.length) return 6;
            while (p+4 <= end) {
                int type = u16(b, p), size = u16(b, p+2); p += 4;
                if (p+size > end) return 6;
                if (type == 0 && size >= 5 && u16(b, p) == size-2) {
                    int name = p+2, nameEnd = p+size;
                    while (name+3 <= nameEnd) {
                        int kind = b[name]&255, length = u16(b, name+1); name += 3;
                        if (name+length > nameEnd) return 6;
                        if (kind == 0 && length > 1) return name+length/2;
                        name += length;
                    }
                }
                p += size;
            }
        } catch (IndexOutOfBoundsException ignored) {}
        return 6;
    }
    private static int u16(byte[] b, int p) { return ((b[p]&255)<<8) | (b[p+1]&255); }
    private static void closeSocket(Socket socket) { try { socket.close(); } catch (IOException ignored) {} }
    @Override public synchronized void close() {
        if (!closed.compareAndSet(false, true)) return;
        try { listener.close(); } catch (IOException ignored) {}
        sockets.forEach(TlsFragmentProxy::closeSocket); tasks.shutdownNow();
    }
}
