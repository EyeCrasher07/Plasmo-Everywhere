# Plasmo Everywhere

A client-side addon for Plasmo Voice that lets nearby players talk on Minecraft servers without a voice chat plugin. Nothing needs to be installed on the Minecraft server.

[Download](https://github.com/EyeCrasher07/Plasmo-Everywhere/releases)

## Requirements

Choose the JAR for your Minecraft version and loader: **Fabric, Forge or NeoForge**. Install the matching **Plasmo Voice** client (2.1.17 or newer where available); Fabric also requires Fabric API. Available combinations are listed with the release files.

Java **17+** is required through Minecraft 1.20.4, **21+** for 1.20.5–1.21.11, and **25+** for 26.x.

## Installation and use

Put the Plasmo Everywhere JAR and its dependencies in your `mods` folder. Everyone you want to talk to needs the addon and Plasmo Voice installed.

Join the same Minecraft server using the same server address. Voice connects automatically. Use the normal Plasmo Voice controls for your microphone, push-to-talk and volume. Open **Esc → Everywhere** to check the connection or reconnect.

The addon tries to connect players directly and uses a relay when a direct connection is unavailable. Servers with their own Plasmo Voice setup take priority automatically.

## Build

Each `<loader>/<minecraft-version>/` folder is a standalone Gradle project. Use its Gradle wrapper and a compatible JDK. For example, with Java 25:

```sh
cd fabric/26.3
./gradlew build
```

The client JAR is written to that folder’s `build/libs/`. Choose the JAR without `sources`, `thin` or `dev` in its name.

## License

[GPL-3.0-only](LICENSE).
