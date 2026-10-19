package dev.eyecrasher.everywhere.smoke;

import net.minecraft.client.Minecraft;
import org.slf4j.LoggerFactory;
import su.plo.voice.client.ModVoiceClient;

/** Observes a real native PV server; never synthesizes its handshake or addon callback. */
public final class NativeSmoke {
    private int ticks, confirmed;
    private boolean done;
    public NativeSmoke() {
        net.neoforged.neoforge.common.NeoForge.EVENT_BUS.addListener(
                (net.neoforged.neoforge.client.event.ClientTickEvent.Post event) -> tick(Minecraft.getInstance()));
    }
    private void tick(Minecraft mc) {
        if(done)return;
        try {
            if(++ticks>1600)throw new AssertionError("Native PV startup timeout");
            var pv=ModVoiceClient.INSTANCE;
            if(mc.player==null || mc.level==null || pv.getServerInfo().isEmpty() || pv.getServerConnection().isEmpty())return;
            Class<?> type=Class.forName("dev.eyecrasher.everywhere.fabric.EverywhereMod");
            Object owner=field(type,null,"instance");
            if(owner==null || !Boolean.TRUE.equals(field(type,owner,"nativeVoice")))return;
            if(field(type,owner,"transport")!=null || field(type,owner,"bridge")!=null)
                throw new AssertionError("Native server retained an external session");
            if(!field(type,owner,"state").toString().equals("NATIVE"))throw new AssertionError("Native state missing");
            var connection=pv.getServerConnection().orElseThrow();
            if(connection.getClass().getName().contains("PlasmoBridge"))throw new AssertionError("Fallback masked native connection");
            if(!pv.getUdpClientManager().getClient().map(client -> client.isConnected()).orElse(false))return;
            if(++confirmed<40)return;
            done=true;
            LoggerFactory.getLogger("EverywhereNativeSmoke").info(
                    "EVERYWHERE_NATIVE_SMOKE PASS native=true externalSession=none nativeUdp=connected confirmedTicks={}",confirmed);
            mc.stop();
        } catch(Throwable failure) {
            done=true;LoggerFactory.getLogger("EverywhereNativeSmoke").error("EVERYWHERE_NATIVE_SMOKE FAIL {}",failure.getClass().getSimpleName());mc.stop();
        }
    }
    private static Object field(Class<?> type,Object owner,String name) throws ReflectiveOperationException {
        var field=type.getDeclaredField(name);field.setAccessible(true);return field.get(owner);
    }
}
