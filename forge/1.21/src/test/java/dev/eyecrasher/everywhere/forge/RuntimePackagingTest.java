package dev.eyecrasher.everywhere.forge;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.nio.file.Path;
import java.util.regex.Pattern;
import java.util.zip.ZipFile;

/** Checks the shipped archive, because a named development launch hides reobfuscation errors. */
public final class RuntimePackagingTest {
    private static final Pattern SRG = Pattern.compile("(?:^|[^A-Za-z0-9_])(?:m_[0-9]+_|f_[0-9]+_)(?=$|[^A-Za-z0-9_])");
    public static void main(String[] args) throws Exception {
        int classes = 0;
        try (var jar = new ZipFile(Path.of(args[0]).toFile())) {
            var config = jar.getEntry("plasmo_everywhere.mixins.json");
            String mixins = new String(jar.getInputStream(config).readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
            if (mixins.contains("\"refmap\"")) throw new AssertionError("Official-runtime JAR must not load an SRG refmap");
            for (var entries = jar.entries(); entries.hasMoreElements();) {
                var entry = entries.nextElement();
                if (!entry.getName().startsWith("dev/eyecrasher/everywhere/forge/") || !entry.getName().endsWith(".class")) continue;
                classes++;
                try (var in = new DataInputStream(new ByteArrayInputStream(jar.getInputStream(entry).readAllBytes()))) {
                    if (in.readInt() != 0xCAFEBABE) throw new AssertionError("Invalid class");
                    in.readUnsignedShort(); in.readUnsignedShort();
                    int count = in.readUnsignedShort();
                    for (int n = 1; n < count; n++) {
                        switch (in.readUnsignedByte()) {
                            case 1 -> {
                                String value = in.readUTF();
                                if (SRG.matcher(value).find()) throw new AssertionError("SRG symbol in official-runtime archive: " + entry.getName() + " -> " + value);
                            }
                            case 3, 4, 9, 10, 11, 12, 17, 18 -> in.skipNBytes(4);
                            case 5, 6 -> { in.skipNBytes(8); n++; }
                            case 7, 8, 16, 19, 20 -> in.skipNBytes(2);
                            case 15 -> in.skipNBytes(3);
                            default -> throw new AssertionError("Unknown constant pool tag");
                        }
                    }
                }
            }
        }
        if (classes == 0) throw new AssertionError("No addon classes in distribution archive");
        System.out.println("Runtime packaging: official Minecraft names, no SRG refmap, " + classes + " addon classes checked");
    }
}
