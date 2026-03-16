package org.shipwrights.krakk.runtime.client;

import java.lang.reflect.Method;

final class KrakkClientShaderPackCompat {
    private static final String[] IRIS_API_CLASSES = new String[]{
            "net.irisshaders.iris.api.v0.IrisApi",
            "net.coderbot.iris.api.v0.IrisApi"
    };
    private static final String[] IRIS_RUNTIME_CLASSES = new String[]{
            "net.irisshaders.iris.Iris",
            "net.coderbot.iris.Iris"
    };
    private static final String[] IRIS_CONFIG_CLASSES = new String[]{
            "net.irisshaders.iris.config.IrisConfig",
            "net.coderbot.iris.config.IrisConfig"
    };
    private static final long IRIS_STATE_REFRESH_INTERVAL_MILLIS = 500L;

    private static volatile long nextIrisStateRefreshMillis = Long.MIN_VALUE;
    private static volatile boolean irisShaderPackActive = false;
    private static volatile boolean irisApiUnavailable = false;
    private static volatile Method irisGetInstanceMethod;
    private static volatile Method irisIsShaderPackInUseMethod;
    private static volatile boolean irisRuntimeMethodsUnavailable = false;
    private static volatile Method irisGetCurrentPackMethod;
    private static volatile Method irisGetConfigMethod;
    private static volatile Method irisConfigAreShadersEnabledMethod;

    private KrakkClientShaderPackCompat() {
    }

    static boolean isShaderPackActive() {
        long now = System.currentTimeMillis();
        if (now < nextIrisStateRefreshMillis) {
            return irisShaderPackActive;
        }

        nextIrisStateRefreshMillis = now + IRIS_STATE_REFRESH_INTERVAL_MILLIS;
        irisShaderPackActive = queryIrisShaderPackState();
        return irisShaderPackActive;
    }

    private static boolean queryIrisShaderPackState() {
        if (resolveIrisApiMethods()) {
            try {
                Object irisApi = irisGetInstanceMethod.invoke(null);
                if (irisApi != null) {
                    Object shaderPackInUse = irisIsShaderPackInUseMethod.invoke(irisApi);
                    if (shaderPackInUse instanceof Boolean bool && bool) {
                        return true;
                    }
                }
            } catch (ReflectiveOperationException | RuntimeException ignored) {
            }
        }

        return queryIrisRuntimeStateFallback();
    }

    private static boolean queryIrisRuntimeStateFallback() {
        if (!resolveIrisRuntimeMethods()) {
            return false;
        }

        try {
            Object currentPack = irisGetCurrentPackMethod.invoke(null);
            if (currentPack instanceof java.util.Optional<?> optional && optional.isPresent()) {
                return true;
            }

            Object irisConfig = irisGetConfigMethod.invoke(null);
            if (irisConfig == null) {
                return false;
            }
            Object shadersEnabled = irisConfigAreShadersEnabledMethod.invoke(irisConfig);
            return shadersEnabled instanceof Boolean bool && bool;
        } catch (ReflectiveOperationException | RuntimeException ignored) {
            return false;
        }
    }

    private static boolean resolveIrisApiMethods() {
        if (irisGetInstanceMethod != null && irisIsShaderPackInUseMethod != null) {
            return true;
        }
        if (irisApiUnavailable) {
            return false;
        }

        for (String irisApiClassName : IRIS_API_CLASSES) {
            try {
                Class<?> irisApiClass = Class.forName(irisApiClassName);
                irisGetInstanceMethod = irisApiClass.getMethod("getInstance");
                irisIsShaderPackInUseMethod = irisApiClass.getMethod("isShaderPackInUse");
                return true;
            } catch (ClassNotFoundException | NoSuchMethodException | LinkageError ignored) {
            }
        }

        irisApiUnavailable = true;
        return false;
    }

    private static boolean resolveIrisRuntimeMethods() {
        if (irisGetCurrentPackMethod != null && irisGetConfigMethod != null && irisConfigAreShadersEnabledMethod != null) {
            return true;
        }
        if (irisRuntimeMethodsUnavailable) {
            return false;
        }

        for (String runtimeClassName : IRIS_RUNTIME_CLASSES) {
            try {
                Class<?> irisClass = Class.forName(runtimeClassName);
                Method getCurrentPack = irisClass.getMethod("getCurrentPack");
                Method getConfig = irisClass.getMethod("getIrisConfig");

                for (String configClassName : IRIS_CONFIG_CLASSES) {
                    try {
                        Class<?> irisConfigClass = Class.forName(configClassName);
                        Method areShadersEnabled = irisConfigClass.getMethod("areShadersEnabled");
                        irisGetCurrentPackMethod = getCurrentPack;
                        irisGetConfigMethod = getConfig;
                        irisConfigAreShadersEnabledMethod = areShadersEnabled;
                        return true;
                    } catch (ClassNotFoundException | NoSuchMethodException | LinkageError ignored) {
                    }
                }
            } catch (ClassNotFoundException | NoSuchMethodException | LinkageError ignored) {
            }
        }

        irisRuntimeMethodsUnavailable = true;
        return false;
    }
}
