package org.shipwrights.krakk.runtime.client;

import com.mojang.blaze3d.shaders.Uniform;
import com.mojang.logging.LogUtils;
import net.minecraft.client.renderer.GameRenderer;
import net.minecraft.client.renderer.ShaderInstance;
import net.minecraft.resources.ResourceLocation;
import org.shipwrights.krakk.Krakk;
import org.slf4j.Logger;

import java.lang.reflect.Method;
import java.nio.FloatBuffer;
import java.util.ArrayList;
import java.util.List;

public final class KrakkClientShaders {
    private static final Logger LOGGER = LogUtils.getLogger();
    public static final ResourceLocation DAMAGE_OVERLAY_SHADER_ID =
            new ResourceLocation(Krakk.MOD_ID, "rendertype_krakk_damage_overlay");
    public static final String DAMAGE_OVERLAY_SHADER_RESOURCE_PATH =
            "assets/krakk/shaders/core/rendertype_krakk_damage_overlay.json";

    private static volatile ShaderInstance damageOverlayFogShader;
    private static volatile boolean fogShaderFailedByRuntime = false;
    private static volatile boolean loggedCustomShaderInvalid = false;
    private static volatile boolean loggedOptionalFogColorMissing = false;
    private static volatile boolean loggedFallbackNoShaderPack = false;
    private static volatile boolean loggedFallbackWithShaderPack = false;
    private static volatile boolean loggedMissingFallbackShader = false;
    private static volatile boolean loggedUsingFogShader = false;
    private static volatile boolean loggedMissingFogShaderInstance = false;
    private static volatile boolean loggedShaderPackForcedFallback = false;
    private static volatile boolean loggedFogSyncMethodUnavailable = false;
    private static volatile boolean fogMethodLookupAttempted = false;
    private static volatile Method renderSystemGetShaderFogStartMethod;
    private static volatile Method renderSystemGetShaderFogEndMethod;
    private static volatile Method renderSystemGetShaderFogColorMethod;
    private static volatile Method renderSystemGetShaderFogShapeMethod;

    private KrakkClientShaders() {
    }

    public static void registerDamageOverlayShader(ShaderInstance shaderInstance) {
        damageOverlayFogShader = shaderInstance;
        krakk$resetShaderSelectionState();
    }

    private static void krakk$resetShaderSelectionState() {
        fogShaderFailedByRuntime = false;
        loggedCustomShaderInvalid = false;
        loggedOptionalFogColorMissing = false;
        loggedFallbackNoShaderPack = false;
        loggedFallbackWithShaderPack = false;
        loggedMissingFallbackShader = false;
        loggedUsingFogShader = false;
        loggedMissingFogShaderInstance = false;
        loggedShaderPackForcedFallback = false;
    }

    public static boolean hasDamageOverlayShaderResource() {
        return krakk$hasShaderResource(DAMAGE_OVERLAY_SHADER_RESOURCE_PATH);
    }

    private static boolean krakk$hasShaderResource(String shaderResourcePath) {
        return KrakkClientShaders.class.getClassLoader().getResource(shaderResourcePath) != null;
    }

    public static ShaderInstance damageOverlayShaderOrFallback() {
        ShaderInstance shaderInstance = krakk$resolveCustomShader();
        if (shaderInstance != null) {
            return shaderInstance;
        }

        ShaderInstance fallbackShader = GameRenderer.getRendertypeCrumblingShader();
        if (fallbackShader != null) {
            krakk$logFallbackReason();
            return fallbackShader;
        }

        if (!loggedMissingFallbackShader) {
            LOGGER.error(
                    "Krakk damage overlay shader unavailable and vanilla fallback shader is null; overlay cannot render."
            );
            loggedMissingFallbackShader = true;
        }
        return null;
    }

    private static ShaderInstance krakk$resolveCustomShader() {
        boolean shaderPackActive = KrakkClientShaderPackCompat.isShaderPackActive();
        if (shaderPackActive) {
            if (!loggedShaderPackForcedFallback) {
                LOGGER.info("Krakk damage overlay shader: shader pack is active; forcing vanilla crumbling shader path.");
                loggedShaderPackForcedFallback = true;
            }
            return null;
        }
        return krakk$resolveUsableShader(damageOverlayFogShader);
    }

    private static ShaderInstance krakk$resolveUsableShader(ShaderInstance shaderInstance) {
        if (shaderInstance == null) {
            if (!loggedMissingFogShaderInstance) {
                LOGGER.warn("Krakk damage overlay shader: fog-capable custom shader instance is unavailable.");
                loggedMissingFogShaderInstance = true;
            }
            return null;
        }

        if (krakk$isFailedByRuntime(shaderInstance)) {
            return null;
        }
        if (!krakk$isCustomShaderUsable(shaderInstance)) {
            return null;
        }

        if (!loggedUsingFogShader) {
            LOGGER.info("Krakk damage overlay shader: using fog-capable custom shader.");
            loggedUsingFogShader = true;
        }
        return shaderInstance;
    }

    private static boolean krakk$isCustomShaderUsable(ShaderInstance shaderInstance) {
        List<String> missing = new ArrayList<>(5);
        krakk$requireUniform(shaderInstance.MODEL_VIEW_MATRIX, "ModelViewMat", missing);
        krakk$requireUniform(shaderInstance.PROJECTION_MATRIX, "ProjMat", missing);
        krakk$requireUniform(shaderInstance.COLOR_MODULATOR, "ColorModulator", missing);
        krakk$requireUniform(shaderInstance.FOG_START, "FogStart", missing);
        krakk$requireUniform(shaderInstance.FOG_END, "FogEnd", missing);
        if (shaderInstance.FOG_COLOR == null && !loggedOptionalFogColorMissing) {
            LOGGER.warn("Krakk fog-capable custom shader is missing optional uniform FogColor; continuing.");
            loggedOptionalFogColorMissing = true;
        }

        if (missing.isEmpty()) {
            return true;
        }

        if (!loggedCustomShaderInvalid) {
            LOGGER.warn(
                    "Krakk custom fog-capable shader is missing required uniforms {}; this shader variant ({}) will be skipped.",
                    missing,
                    krakk$shaderDebugName(shaderInstance)
            );
            loggedCustomShaderInvalid = true;
        }
        return false;
    }

    private static void krakk$requireUniform(Uniform uniform, String name, List<String> missing) {
        if (uniform == null) {
            missing.add(name);
        }
    }

    private static void krakk$logFallbackReason() {
        boolean shaderPackActive = KrakkClientShaderPackCompat.isShaderPackActive();
        boolean fogResourcePresent = hasDamageOverlayShaderResource();
        String issueDetail;
        if (shaderPackActive && loggedShaderPackForcedFallback) {
            issueDetail = "custom shader path disabled under active shader packs";
        } else if (loggedCustomShaderInvalid) {
            issueDetail = "custom shader variant missing required uniforms";
        } else if (fogShaderFailedByRuntime) {
            issueDetail = "custom shader variant failed during rendering and was disabled";
        } else if (fogResourcePresent) {
            issueDetail = "custom shader resource is present, but no usable custom shader instance loaded";
        } else {
            issueDetail = "custom shader resource missing at " + DAMAGE_OVERLAY_SHADER_RESOURCE_PATH;
        }
        if (shaderPackActive) {
            if (!loggedFallbackWithShaderPack) {
                LOGGER.warn(
                        "Krakk custom damage shader unavailable while shader pack is active ({}); falling back to vanilla crumbling shader.",
                        issueDetail
                );
                loggedFallbackWithShaderPack = true;
            }
            return;
        }

        if (!loggedFallbackNoShaderPack) {
            LOGGER.warn(
                    "Krakk custom damage shader unavailable ({}); falling back to vanilla crumbling shader.",
                    issueDetail
            );
            loggedFallbackNoShaderPack = true;
        }
    }

    public static boolean isRegisteredCustomShader(ShaderInstance shaderInstance) {
        return shaderInstance != null && shaderInstance == damageOverlayFogShader;
    }

    public static void syncFogUniforms(ShaderInstance shaderInstance) {
        if (shaderInstance == null) {
            return;
        }

        krakk$resolveFogMethods();
        if (renderSystemGetShaderFogStartMethod == null || renderSystemGetShaderFogEndMethod == null) {
            if (!loggedFogSyncMethodUnavailable) {
                LOGGER.warn("Krakk damage overlay shader fog sync unavailable: RenderSystem fog getter methods were not found.");
                loggedFogSyncMethodUnavailable = true;
            }
            return;
        }

        try {
            if (shaderInstance.FOG_START != null) {
                Object fogStart = renderSystemGetShaderFogStartMethod.invoke(null);
                if (fogStart instanceof Number number) {
                    shaderInstance.FOG_START.set(number.floatValue());
                }
            }
            if (shaderInstance.FOG_END != null) {
                Object fogEnd = renderSystemGetShaderFogEndMethod.invoke(null);
                if (fogEnd instanceof Number number) {
                    shaderInstance.FOG_END.set(number.floatValue());
                }
            }
            if (shaderInstance.FOG_COLOR != null && renderSystemGetShaderFogColorMethod != null) {
                Object fogColor = renderSystemGetShaderFogColorMethod.invoke(null);
                float[] values = krakk$extractFogColorValues(fogColor);
                if (values != null) {
                    shaderInstance.FOG_COLOR.set(values[0], values[1], values[2], values[3]);
                }
            }
            Uniform fogShapeUniform = shaderInstance.getUniform("FogShape");
            if (fogShapeUniform != null && renderSystemGetShaderFogShapeMethod != null) {
                Object fogShape = renderSystemGetShaderFogShapeMethod.invoke(null);
                Integer fogShapeValue = krakk$extractFogShapeValue(fogShape);
                if (fogShapeValue != null) {
                    fogShapeUniform.set(fogShapeValue);
                }
            }

        } catch (ReflectiveOperationException | RuntimeException ignored) {
        }
    }

    public static void markCustomShaderRenderFailure(ShaderInstance shaderInstance, RuntimeException exception) {
        if (!isRegisteredCustomShader(shaderInstance) || krakk$isFailedByRuntime(shaderInstance)) {
            return;
        }
        fogShaderFailedByRuntime = true;
        LOGGER.error(
                "Krakk custom damage shader {} failed during rendering; disabling this shader variant.",
                krakk$shaderDebugName(shaderInstance),
                exception
        );
    }

    private static boolean krakk$isFailedByRuntime(ShaderInstance shaderInstance) {
        return shaderInstance == damageOverlayFogShader && fogShaderFailedByRuntime;
    }

    private static String krakk$shaderDebugName(ShaderInstance shaderInstance) {
        if (shaderInstance == damageOverlayFogShader) {
            return DAMAGE_OVERLAY_SHADER_ID.toString();
        }
        return "<unknown>";
    }

    private static void krakk$resolveFogMethods() {
        if (fogMethodLookupAttempted) {
            return;
        }
        fogMethodLookupAttempted = true;

        try {
            Class<?> renderSystemClass = Class.forName("com.mojang.blaze3d.systems.RenderSystem");
            renderSystemGetShaderFogStartMethod = renderSystemClass.getMethod("getShaderFogStart");
            renderSystemGetShaderFogEndMethod = renderSystemClass.getMethod("getShaderFogEnd");

            try {
                renderSystemGetShaderFogColorMethod = renderSystemClass.getMethod("getShaderFogColor");
            } catch (NoSuchMethodException ignored) {
                renderSystemGetShaderFogColorMethod = null;
            }
            try {
                renderSystemGetShaderFogShapeMethod = renderSystemClass.getMethod("getShaderFogShape");
            } catch (NoSuchMethodException ignored) {
                renderSystemGetShaderFogShapeMethod = null;
            }
        } catch (ReflectiveOperationException | LinkageError ignored) {
            renderSystemGetShaderFogStartMethod = null;
            renderSystemGetShaderFogEndMethod = null;
            renderSystemGetShaderFogColorMethod = null;
            renderSystemGetShaderFogShapeMethod = null;
        }
    }

    private static float[] krakk$extractFogColorValues(Object fogColor) {
        if (fogColor instanceof float[] values && values.length >= 4) {
            return new float[]{values[0], values[1], values[2], values[3]};
        }
        if (fogColor instanceof FloatBuffer buffer && buffer.remaining() >= 4) {
            int position = buffer.position();
            return new float[]{
                    buffer.get(position),
                    buffer.get(position + 1),
                    buffer.get(position + 2),
                    buffer.get(position + 3)
            };
        }
        if (fogColor != null) {
            try {
                Method x = fogColor.getClass().getMethod("x");
                Method y = fogColor.getClass().getMethod("y");
                Method z = fogColor.getClass().getMethod("z");
                Method w = fogColor.getClass().getMethod("w");
                Object xValue = x.invoke(fogColor);
                Object yValue = y.invoke(fogColor);
                Object zValue = z.invoke(fogColor);
                Object wValue = w.invoke(fogColor);
                if (xValue instanceof Number xNumber
                        && yValue instanceof Number yNumber
                        && zValue instanceof Number zNumber
                        && wValue instanceof Number wNumber) {
                    return new float[]{
                            xNumber.floatValue(),
                            yNumber.floatValue(),
                            zNumber.floatValue(),
                            wNumber.floatValue()
                    };
                }
            } catch (ReflectiveOperationException ignored) {
            }
        }
        return null;
    }

    private static Integer krakk$extractFogShapeValue(Object fogShape) {
        if (fogShape instanceof Number number) {
            return number.intValue();
        }
        if (fogShape instanceof Enum<?> enumValue) {
            return enumValue.ordinal();
        }
        return null;
    }
}
