package org.shipwrights.krakk.fabric;

import net.fabricmc.api.ModInitializer;
import net.fabricmc.fabric.api.event.lifecycle.v1.ServerTickEvents;
import org.shipwrights.krakk.Krakk;
import org.shipwrights.krakk.vs2.KrakkVS2Support;

public final class KrakkFabric implements ModInitializer {
    @Override
    public void onInitialize() {
        Krakk.init();
        if (KrakkVS2Support.isPresent()) {
            KrakkVS2Support.init();
            ServerTickEvents.END_SERVER_TICK.register(KrakkVS2Support::drainCollisions);
        }
    }
}

