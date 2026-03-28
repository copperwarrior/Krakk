package org.shipwrights.krakk.forge;

import dev.architectury.platform.forge.EventBuses;
import net.minecraftforge.common.MinecraftForge;
import net.minecraftforge.event.TickEvent;
import net.minecraftforge.eventbus.api.IEventBus;
import net.minecraftforge.fml.common.Mod;
import net.minecraftforge.fml.javafmlmod.FMLJavaModLoadingContext;
import org.shipwrights.krakk.Krakk;
import org.shipwrights.krakk.api.KrakkApi;
import org.shipwrights.krakk.forge.client.KrakkForgeClient;
import org.shipwrights.krakk.vs2.KrakkVS2Support;
import net.minecraftforge.fml.loading.FMLEnvironment;

@Mod(Krakk.MOD_ID)
public final class KrakkForge {
    public KrakkForge() {
        IEventBus modEventBus = FMLJavaModLoadingContext.get().getModEventBus();
        EventBuses.registerModEventBus(Krakk.MOD_ID, modEventBus);
        Krakk.init();
        if (KrakkVS2Support.isPresent()) {
            KrakkVS2Support.init();
            MinecraftForge.EVENT_BUS.addListener(KrakkForge::onServerTick);
        }
        if (FMLEnvironment.dist.isClient()) {
            KrakkForgeClient.init(modEventBus);
            KrakkApi.network().initClientReceivers();
        }
    }

    private static void onServerTick(TickEvent.ServerTickEvent event) {
        if (event.phase != TickEvent.Phase.END) return;
        KrakkVS2Support.drainCollisions(event.getServer());
    }
}
