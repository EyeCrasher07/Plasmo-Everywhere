package dev.eyecrasher.everywhere.core;

import com.google.gson.JsonObject;
import java.net.*;

/** Numeric, public unicast candidates only. Never resolves a peer-supplied hostname. */
public final class Endpoints {
    private Endpoints() {}
    public static InetSocketAddress read(JsonObject json, boolean localTest) {
        try {
            String ip = Wire.string(json, "ip", 45);
            if (ip.contains(":")) {
                if (!ip.matches("[0-9a-fA-F:]+")) throw new IllegalArgumentException("Not an IPv6 literal");
            } else {
                if (!ip.matches("[0-9]{1,3}(\\.[0-9]{1,3}){3}")) throw new IllegalArgumentException("Not an IPv4 literal");
                for (String part : ip.split("\\.")) if (Integer.parseInt(part) > 255) throw new IllegalArgumentException("IPv4 range");
            }
            InetAddress address = InetAddress.getByName(ip);
            int port = json.get("port").getAsInt();
            if (port < 1 || port > 65535 || !(isPublic(address) || (localTest && address.isLoopbackAddress())))
                throw new IllegalArgumentException("Non-public candidate");
            return new InetSocketAddress(address, port);
        } catch (UnknownHostException e) { throw new IllegalArgumentException("Invalid IP literal"); }
    }
    public static JsonObject write(InetSocketAddress endpoint) {
        JsonObject j = new JsonObject(); j.addProperty("ip", endpoint.getAddress().getHostAddress());
        j.addProperty("port", endpoint.getPort()); return j;
    }
    public static boolean isPublic(InetAddress address) {
        if (address.isAnyLocalAddress() || address.isLoopbackAddress() || address.isSiteLocalAddress()
                || address.isLinkLocalAddress() || address.isMulticastAddress()) return false;
        byte[] a = address.getAddress(); int first = a[0] & 255, second = a[1] & 255;
        if (a.length == 16) {
            // Global unicast; exclude the documentation prefix too.
            return (first & 0xe0) == 0x20 && !(first == 0x20 && second == 1 && (a[2] & 255) == 13 && (a[3] & 255) == 184);
        }
        return first != 0 && first < 224 && !(first == 100 && second >= 64 && second <= 127)
                && !(first == 192 && (second == 0 || second == 2))
                && !(first == 198 && (second == 18 || second == 19 || (second == 51 && (a[2] & 255) == 100)))
                && !(first == 203 && second == 0 && (a[2] & 255) == 113);
    }
}
