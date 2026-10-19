package dev.eyecrasher.everywhere.core;

import java.io.*;
import java.nio.file.*;
import java.security.*;
import java.security.cert.CertificateFactory;
import javax.net.ssl.*;

public final class Tls {
    private Tls() {}
    /** Trust only the project's public development certificate; never disable TLS verification. */
    public static SSLContext pinned(InputStream certificate) throws Exception {
        if (certificate == null) throw new IOException("Relay certificate missing");
        KeyStore store = KeyStore.getInstance(KeyStore.getDefaultType()); store.load(null);
        try (certificate) {
            store.setCertificateEntry("relay", CertificateFactory.getInstance("X.509").generateCertificate(certificate));
        }
        TrustManagerFactory trust = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm()); trust.init(store);
        SSLContext context = SSLContext.getInstance("TLS"); context.init(null, trust.getTrustManagers(), null);
        return context;
    }
    public static SSLContext server(Path keystore, Path passwordFile) throws Exception {
        char[] password = Files.readString(passwordFile).strip().toCharArray();
        try {
            KeyStore store = KeyStore.getInstance("PKCS12");
            try (InputStream in = Files.newInputStream(keystore)) { store.load(in, password); }
            KeyManagerFactory keys = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm()); keys.init(store, password);
            SSLContext context = SSLContext.getInstance("TLS"); context.init(keys.getKeyManagers(), null, null); return context;
        } finally { java.util.Arrays.fill(password, '\0'); }
    }
}
