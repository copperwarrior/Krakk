package org.shipwrights.krakk.forge;

import dev.architectury.platform.forge.EventBuses;
import net.minecraftforge.fml.common.Mod;
import net.minecraftforge.fml.javafmlmod.FMLJavaModLoadingContext;
import net.minecraftforge.eventbus.api.IEventBus;
import org.shipwrights.krakk.Krakk;
import org.shipwrights.krakk.api.KrakkApi;
import org.shipwrights.krakk.forge.client.KrakkForgeClient;
import net.minecraftforge.fml.loading.FMLEnvironment;

@Mod(Krakk.MOD_ID)
public final class KrakkForge {
    public KrakkForge() {
        IEventBus modEventBus = FMLJavaModLoadingContext.get().getModEventBus();
        EventBuses.registerModEventBus(Krakk.MOD_ID, modEventBus);
        Krakk.init();
        if (FMLEnvironment.dist.isClient()) {
            KrakkForgeClient.init(modEventBus);
            KrakkApi.network().initClientReceivers();
        }
    }
}
