package org.shipwrights.krakk.forge.client;

import com.mojang.blaze3d.vertex.DefaultVertexFormat;
import com.mojang.logging.LogUtils;
import net.minecraft.client.renderer.ShaderInstance;
import net.minecraftforge.client.event.RegisterShadersEvent;
import net.minecraftforge.eventbus.api.IEventBus;
import org.shipwrights.krakk.runtime.client.KrakkClientShaders;
import org.slf4j.Logger;

import java.io.IOException;

public final class KrakkForgeClient {
    private static final Logger LOGGER = LogUtils.getLogger();

    private KrakkForgeClient() {
    }

    public static void init(IEventBus modEventBus) {
        modEventBus.addListener(KrakkForgeClient::onRegisterShaders);
    }

    private static void onRegisterShaders(RegisterShadersEvent event) {
        boolean hasFogShaderResource = KrakkClientShaders.hasDamageOverlayShaderResource();
        if (!hasFogShaderResource) {
            LOGGER.warn("Krakk damage overlay shader resource missing at {}",
                    KrakkClientShaders.DAMAGE_OVERLAY_SHADER_RESOURCE_PATH);
            return;
        }

        try {
            ShaderInstance shaderInstance = new ShaderInstance(
                    event.getResourceProvider(),
                    KrakkClientShaders.DAMAGE_OVERLAY_SHADER_ID,
                    DefaultVertexFormat.BLOCK
            );
            event.registerShader(shaderInstance, KrakkClientShaders::registerDamageOverlayShader);
        } catch (IOException exception) {
            LOGGER.error("Failed to register Krakk damage overlay shader {}", KrakkClientShaders.DAMAGE_OVERLAY_SHADER_ID, exception);
        }
    }
}
