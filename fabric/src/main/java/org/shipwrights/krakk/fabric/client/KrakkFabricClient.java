package org.shipwrights.krakk.fabric.client;

import com.mojang.blaze3d.vertex.DefaultVertexFormat;
import com.mojang.logging.LogUtils;
import net.fabricmc.fabric.api.client.rendering.v1.CoreShaderRegistrationCallback;
import net.fabricmc.api.ClientModInitializer;
import org.shipwrights.krakk.Krakk;
import org.shipwrights.krakk.api.KrakkApi;
import org.shipwrights.krakk.runtime.client.KrakkClientShaders;
import org.slf4j.Logger;

public final class KrakkFabricClient implements ClientModInitializer {
    private static final Logger LOGGER = LogUtils.getLogger();

    @Override
    public void onInitializeClient() {
        Krakk.init();
        boolean hasFogShaderResource = KrakkClientShaders.hasDamageOverlayShaderResource();
        if (!hasFogShaderResource) {
            LOGGER.warn("Krakk damage overlay shader resource missing at {}", KrakkClientShaders.DAMAGE_OVERLAY_SHADER_RESOURCE_PATH);
            KrakkApi.network().initClientReceivers();
            return;
        }
        CoreShaderRegistrationCallback.EVENT.register(context -> {
            context.register(
                    KrakkClientShaders.DAMAGE_OVERLAY_SHADER_ID,
                    DefaultVertexFormat.BLOCK,
                    KrakkClientShaders::registerDamageOverlayShader
            );
        });
        KrakkApi.network().initClientReceivers();
    }
}
