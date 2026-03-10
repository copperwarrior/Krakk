package org.shipwrights.krakk.forge;

import dev.architectury.platform.forge.EventBuses;
import net.minecraftforge.fml.common.Mod;
import net.minecraftforge.fml.javafmlmod.FMLJavaModLoadingContext;
import org.shipwrights.krakk.Krakk;
import org.shipwrights.krakk.api.KrakkApi;
import net.minecraftforge.fml.loading.FMLEnvironment;

@Mod(Krakk.MOD_ID)
public final class KrakkForge {
    public KrakkForge() {
        EventBuses.registerModEventBus(Krakk.MOD_ID, FMLJavaModLoadingContext.get().getModEventBus());
        Krakk.init();
        if (FMLEnvironment.dist.isClient()) {
            KrakkApi.network().initClientReceivers();
        }
    }
}
