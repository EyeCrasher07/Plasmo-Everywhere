# Third-party notices

Plasmo Everywhere's original code and documentation are licensed under
**GNU GPL version 3 only** (`GPL-3.0-only`); see [LICENSE](../LICENSE).
Third-party components retain their respective licenses and copyright notices.

## Client addon

The client JAR includes Everywhere's `core` module. Plasmo Voice, the mod loader,
Fabric API (on Fabric), Minecraft and Java are installed separately.

Fabric and Forge client JARs also bundle Google Gson **2.11.0** and Error Prone
annotations **2.27.0**, under Apache-2.0, in a private package to avoid conflicting
with the game's libraries. Their license texts accompany these JARs in
`META-INF/licenses/`. NeoForge clients use Gson provided by the game.

## Standalone relay

The standalone relay JAR bundles the following dependencies:

| Component | Version | Use | License |
| --- | --- | --- | --- |
| Google Gson | 2.11.0 | JSON | [Apache-2.0](Gson-2.11.0-Apache-2.0.txt) |
| Error Prone annotations | 2.27.0 | Transitive Gson dependency | [Apache-2.0](ErrorProneAnnotations-2.27.0-Apache-2.0.txt) |
| Java-WebSocket | 1.6.0 | WebSocket server | [MIT](Java-WebSocket-1.6.0-MIT.txt) |
| SLF4J API and SLF4J Simple | 2.0.17 | Logging | [MIT](SLF4J-2.0.17-MIT.txt) |

The linked license files preserve the upstream notices, including
Copyright (c) 2010–2020 Nathan Rajlich for Java-WebSocket and
Copyright (c) 2004–2022 QOS.ch Sarl (Switzerland) for SLF4J.
These notices and license texts must accompany a distributed relay JAR.
Gradle Wrapper and build plugins are not bundled in the client JAR.

## Separately installed integration

[Plasmo Voice](https://github.com/plasmoapp/plasmo-voice) is a separate mod under
[GNU LGPL v3](https://github.com/plasmoapp/plasmo-voice/blob/main/LICENSE).
Everywhere integrates with the matching Plasmo Voice **2.x** client and does not
bundle its JAR. The [project icon](../artwork/icon.png) was provided by the owner.

The optional TLS-record splitting implementation was written independently,
inspired by the approach documented in
[zapret (`tlsrec`)](https://github.com/bol-van/zapret/blob/master/docs/readme.md#tlsrec)
and [ByeDPI (`--tlsrec`)](https://github.com/hufrea/byedpi#--tlsrec).
GoodbyeDPI, WinDivert, winws, zapret and ByeDPI binaries and scripts are not bundled
or launched by Everywhere.
