package org.shipwrights.krakk;

import org.shipwrights.krakk.api.KrakkApi;
import org.shipwrights.krakk.runtime.damage.KrakkDamageRuntime;
import org.shipwrights.krakk.runtime.explosion.KrakkExplosionRuntime;

/**
 * Krakk module bootstrap.
 * Registers runtime implementations for Krakk APIs.
 */
public final class Krakk {
    private Krakk() {
    }

    /**
     * Installs default runtime API implementations.
     * Call once during mod bootstrap on both loaders.
     */
    public static void init() {
        KrakkApi.setDamageApi(new KrakkDamageRuntime());
        KrakkApi.setExplosionApi(new KrakkExplosionRuntime());
    }
}
