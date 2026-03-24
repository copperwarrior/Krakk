package org.shipwrights.krakk;

import dev.architectury.event.events.common.PlayerEvent;
import dev.architectury.event.events.common.TickEvent;
import dev.architectury.platform.Platform;
import dev.architectury.utils.Env;
import org.shipwrights.krakk.api.KrakkApi;
import org.shipwrights.krakk.command.KrakkDebugCommands;
import org.shipwrights.krakk.network.KrakkBlockDamageNetwork;
import org.shipwrights.krakk.runtime.client.KrakkClientOverlayRuntime;
import org.shipwrights.krakk.runtime.damage.KrakkDamageBlockConversions;
import org.shipwrights.krakk.runtime.damage.KrakkImpactPlacements;
import org.shipwrights.krakk.runtime.damage.KrakkDamageRuntime;
import org.shipwrights.krakk.runtime.explosion.KrakkExplosionRuntime;

/**
 * Krakk module bootstrap.
 * Registers runtime implementations for Krakk APIs.
 */
public final class Krakk {
    public static final String MOD_ID = "krakk";
    private static boolean initialized = false;

    private Krakk() {
    }

    /**
     * Installs default runtime API implementations.
     * Call once during mod bootstrap on both loaders.
     */
    public static void init() {
        if (initialized) {
            return;
        }
        initialized = true;

        KrakkDamageBlockConversions.init();
        KrakkImpactPlacements.init();

        KrakkApi.setDamageApi(new KrakkDamageRuntime());
        KrakkDamageRuntime.setDamageStateConversionHandler(KrakkDamageBlockConversions::applyConversionForDamageState);
        KrakkApi.setExplosionApi(new KrakkExplosionRuntime());
        KrakkApi.setNetworkApi(new KrakkBlockDamageNetwork(MOD_ID));
        if (Platform.getEnvironment() == Env.CLIENT) {
            KrakkApi.setClientOverlayApi(new KrakkClientOverlayRuntime());
            KrakkApi.network().initClientReceivers();
        }
        KrakkDebugCommands.register();
        PlayerEvent.PLAYER_QUIT.register(player -> KrakkApi.damage().clearQueuedPlayerSync(player));
        TickEvent.SERVER_POST.register(server -> {
            KrakkApi.damage().tickQueuedSyncs(server);
            KrakkDebugCommands.tickQueuedDetonations(server);
            KrakkExplosionRuntime.tickQueuedWavefrontJobs(server);
        });
    }
}
