package org.shipwrights.krakk.fabric.client;

import net.fabricmc.api.ClientModInitializer;
import org.shipwrights.krakk.Krakk;
import org.shipwrights.krakk.api.KrakkApi;

public final class KrakkFabricClient implements ClientModInitializer {
    @Override
    public void onInitializeClient() {
        Krakk.init();
        KrakkApi.network().initClientReceivers();
    }
}
