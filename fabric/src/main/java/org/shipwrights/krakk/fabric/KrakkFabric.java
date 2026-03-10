package org.shipwrights.krakk.fabric;

import net.fabricmc.api.ModInitializer;
import org.shipwrights.krakk.Krakk;

public final class KrakkFabric implements ModInitializer {
    @Override
    public void onInitialize() {
        Krakk.init();
    }
}

