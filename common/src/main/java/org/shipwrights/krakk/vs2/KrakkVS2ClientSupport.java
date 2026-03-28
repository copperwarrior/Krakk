package org.shipwrights.krakk.vs2;

import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import net.minecraft.resources.ResourceLocation;
import net.minecraft.client.Minecraft;
import org.joml.Matrix4d;
import org.joml.Matrix4dc;
import org.joml.Matrix4f;
import org.shipwrights.krakk.api.KrakkApi;
import org.shipwrights.krakk.runtime.client.SectionRenderCache;
import org.valkyrienskies.core.api.ships.ClientShip;
import org.valkyrienskies.core.api.ships.properties.ChunkClaim;
import org.valkyrienskies.core.api.world.ClientShipWorld;
import org.valkyrienskies.mod.api.ValkyrienSkies;

/**
 * Client-side VS2 integration for Krakk's damage-overlay rendering.
 *
 * <p>VS2 delivers ship-chunk data to clients as part of the player's current level (the
 * overworld), at shipyard chunk coordinates.  Krakk's overlay rendering pipeline uses a
 * camera-centred {@link org.shipwrights.krakk.runtime.client.VecRange} that does not cover
 * these far-offset coordinates, causing ship damage sections to be discarded or swept.
 *
 * <p>Two helpers are provided for use by {@code KrakkLevelRendererMixin}:
 * <ul>
 *   <li>{@link #addShipSectionKeys} — injects damaged ship section keys into the
 *       active-visible set so they pass the Krakk visibility filter.</li>
 *   <li>{@link #isSectionInShipRange} — range bypass used by the rebuild, sweep, and
 *       collect passes so ship sections are not culled by the camera VecRange.</li>
 * </ul>
 *
 * <p>All VS2 class references are guarded behind {@link KrakkVS2Support#isPresent()}.
 */
public final class KrakkVS2ClientSupport {

    private KrakkVS2ClientSupport() {
    }

    /**
     * Queries Krakk's client overlay store for damaged sections in every loaded VS2
     * ship's chunk claim and adds their section keys to {@code outSectionKeys}.
     *
     * <p>This ensures that ship sections pass the vanilla/Sodium visibility filter in
     * Krakk's section-collect pass, even though vanilla rendering never marks them visible.
     *
     * @param activeDimId  the dimension currently tracked by Krakk's overlay runtime
     * @param outSectionKeys  set to augment with ship section keys
     */
    public static void addShipSectionKeys(ResourceLocation activeDimId,
                                          LongOpenHashSet outSectionKeys) {
        ClientShipWorld sw = ValkyrienSkies.getShipWorld(Minecraft.getInstance());
        if (sw == null) return;

        for (ClientShip ship : sw.getAllShips()) {
            ChunkClaim claim = ship.getChunkClaim();
            long[] sections = KrakkApi.clientOverlay().snapshotSectionsInChunkRange(
                    activeDimId,
                    claim.getXStart(), claim.getXEnd(),
                    claim.getZStart(), claim.getZEnd());
            for (long sectionKey : sections) {
                outSectionKeys.add(sectionKey);
            }
        }
    }

    /**
     * Returns {@code true} if the section at ({@code sectionX}, {@code sectionZ}) lies
     * within any currently loaded VS2 ship's chunk claim.
     *
     * <p>Used by the rebuild, sweep, and collect passes of {@code KrakkLevelRendererMixin}
     * to exempt ship sections from the camera-centred VecRange cull.
     */
    public static boolean isSectionInShipRange(int sectionX, int sectionZ) {
        ClientShipWorld sw = ValkyrienSkies.getShipWorld(Minecraft.getInstance());
        if (sw == null) return false;

        for (ClientShip ship : sw.getAllShips()) {
            ChunkClaim claim = ship.getChunkClaim();
            if (sectionX >= claim.getXStart() && sectionX <= claim.getXEnd()
                    && sectionZ >= claim.getZStart() && sectionZ <= claim.getZEnd()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Computes and writes the correct model-view matrix for drawing a VS2 ship section's
     * damage overlay into {@code outModelViewMatrix}.
     *
     * <p>Ship sections live at shipyard chunk coordinates (large offsets, e.g. -1790848).
     * A naive {@code baseModelViewMatrix.translate(originBlock - camera)} produces a
     * ~28M-block float offset that falls outside the view frustum. This method instead
     * applies VS2's {@code ShipToWorld} transform in double precision so the overlay
     * appears at the ship's actual visual position.
     *
     * <p>The resulting matrix is equivalent to:
     * {@code baseModelViewMatrix * T(-camera) * ShipToWorld * T(sectionOrigin)}.
     *
     * @param cache               the section render cache whose origin is in shipyard coords
     * @param cameraX             camera world-space X
     * @param cameraY             camera world-space Y
     * @param cameraZ             camera world-space Z
     * @param baseModelViewMatrix the view matrix from the PoseStack (not modified)
     * @param outModelViewMatrix  receives the composed model-view matrix
     * @return {@code true} if the section belongs to a loaded ship and the matrix was set;
     *         {@code false} if no owning ship was found (caller should use default translate)
     */
    public static boolean applyShipSectionModelView(
            SectionRenderCache cache,
            double cameraX, double cameraY, double cameraZ,
            Matrix4f baseModelViewMatrix,
            Matrix4f outModelViewMatrix) {
        ClientShipWorld sw = ValkyrienSkies.getShipWorld(Minecraft.getInstance());
        if (sw == null) return false;

        int sectionX = cache.sectionX();
        int sectionZ = cache.sectionZ();

        for (ClientShip ship : sw.getAllShips()) {
            ChunkClaim claim = ship.getChunkClaim();
            if (sectionX < claim.getXStart() || sectionX > claim.getXEnd()
                    || sectionZ < claim.getZStart() || sectionZ > claim.getZEnd()) {
                continue;
            }
            // Found the owning ship. Build: T(-camera) * ShipToWorld * T(sectionOrigin)
            // in double precision to avoid float overflow at large shipyard coordinates.
            Matrix4dc stw = ship.getRenderTransform().getShipToWorld();
            Matrix4d shipMat = new Matrix4d()
                    .translate(-cameraX, -cameraY, -cameraZ)
                    .mul(stw)
                    .translate(cache.originBlockX(), cache.originBlockY(), cache.originBlockZ());
            outModelViewMatrix.set(baseModelViewMatrix).mul(new Matrix4f(shipMat));
            return true;
        }
        return false;
    }
}
