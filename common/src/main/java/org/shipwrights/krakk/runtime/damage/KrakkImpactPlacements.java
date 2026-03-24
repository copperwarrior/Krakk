package org.shipwrights.krakk.runtime.damage;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mojang.logging.LogUtils;
import dev.architectury.registry.ReloadListenerRegistry;
import net.minecraft.core.BlockPos;
import net.minecraft.core.Direction;
import net.minecraft.core.registries.BuiltInRegistries;
import net.minecraft.resources.ResourceLocation;
import net.minecraft.server.level.ServerLevel;
import net.minecraft.server.packs.PackType;
import net.minecraft.server.packs.resources.ResourceManager;
import net.minecraft.server.packs.resources.SimpleJsonResourceReloadListener;
import net.minecraft.util.GsonHelper;
import net.minecraft.util.profiling.ProfilerFiller;
import net.minecraft.world.level.block.Block;
import net.minecraft.world.level.block.state.BlockState;
import org.shipwrights.krakk.Krakk;
import org.shipwrights.krakk.api.damage.KrakkDamageApi;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public final class KrakkImpactPlacements {
    private static final Logger LOGGER = LogUtils.getLogger();
    private static final Gson GSON = new GsonBuilder().disableHtmlEscaping().create();
    private static final String RULE_DIRECTORY = "krakk_impact_placements";
    private static final ResourceLocation RELOAD_LISTENER_ID = new ResourceLocation(Krakk.MOD_ID, RULE_DIRECTORY);
    private static final RuleReloadListener RELOAD_LISTENER = new RuleReloadListener();
    private static final Direction[] AXIS_DIRECTIONS = new Direction[]{
            Direction.WEST, Direction.EAST,
            Direction.DOWN, Direction.UP,
            Direction.NORTH, Direction.SOUTH
    };
    private static volatile List<ImpactPlacementRule> rules = List.of();
    private static boolean initialized;

    private KrakkImpactPlacements() {
    }

    public static void init() {
        if (initialized) {
            return;
        }
        initialized = true;
        ReloadListenerRegistry.register(PackType.SERVER_DATA, RELOAD_LISTENER, RELOAD_LISTENER_ID);
    }

    public static boolean tryPlaceFromImpact(ServerLevel level, BlockPos impactPos, double impactPower, double impactHeatCelsius) {
        if (level == null || impactPos == null || !level.isInWorldBounds(impactPos)) {
            return false;
        }
        double sanitizedHeat = sanitizeHeat(impactHeatCelsius);
        for (ImpactPlacementRule rule : rules) {
            if (!rule.matches(sanitizedHeat)) {
                continue;
            }
            double chance = rule.chanceModel().resolveChance(sanitizedHeat, impactPower);
            if (chance <= 0.0D) {
                continue;
            }
            if (chance < 1.0D && level.random.nextDouble() > chance) {
                continue;
            }
            if (placeBlock(level, impactPos, rule)) {
                return true;
            }
        }
        return false;
    }

    private static boolean placeBlock(ServerLevel level, BlockPos origin, ImpactPlacementRule rule) {
        return switch (rule.placementMode()) {
            case UP_IF_OPEN -> tryPlaceUpIfOpen(level, origin, rule.targetState());
            case RANDOM_ADJACENT -> tryPlaceRandomAdjacent(level, origin, rule.targetState());
            case RANDOM_OPEN_AXIS -> tryPlaceRandomOpenAxis(level, origin, rule.targetState());
        };
    }

    private static boolean tryPlaceUpIfOpen(ServerLevel level, BlockPos origin, BlockState targetState) {
        return tryPlaceAt(level, origin.above(), targetState);
    }

    private static boolean tryPlaceRandomAdjacent(ServerLevel level, BlockPos origin, BlockState targetState) {
        Direction[] shuffled = shuffledDirections(level);
        for (Direction direction : shuffled) {
            if (tryPlaceAt(level, origin.relative(direction), targetState)) {
                return true;
            }
        }
        return false;
    }

    private static boolean tryPlaceRandomOpenAxis(ServerLevel level, BlockPos origin, BlockState targetState) {
        Direction.Axis[] axes = shuffledAxes(level);
        for (Direction.Axis axis : axes) {
            Direction negative = axis == Direction.Axis.X ? Direction.WEST
                    : axis == Direction.Axis.Y ? Direction.DOWN : Direction.NORTH;
            Direction positive = axis == Direction.Axis.X ? Direction.EAST
                    : axis == Direction.Axis.Y ? Direction.UP : Direction.SOUTH;

            BlockPos negativePos = origin.relative(negative);
            BlockPos positivePos = origin.relative(positive);
            boolean canPlaceNegative = canPlaceAt(level, negativePos, targetState);
            boolean canPlacePositive = canPlaceAt(level, positivePos, targetState);
            if (!canPlaceNegative && !canPlacePositive) {
                continue;
            }
            if (canPlaceNegative && canPlacePositive) {
                return level.random.nextBoolean()
                        ? tryPlaceAt(level, negativePos, targetState)
                        : tryPlaceAt(level, positivePos, targetState);
            }
            return canPlaceNegative
                    ? tryPlaceAt(level, negativePos, targetState)
                    : tryPlaceAt(level, positivePos, targetState);
        }
        return false;
    }

    private static boolean tryPlaceAt(ServerLevel level, BlockPos pos, BlockState targetState) {
        if (!canPlaceAt(level, pos, targetState)) {
            return false;
        }
        return level.setBlock(pos, targetState, 3);
    }

    private static boolean canPlaceAt(ServerLevel level, BlockPos pos, BlockState targetState) {
        if (!level.isInWorldBounds(pos)) {
            return false;
        }
        BlockState existingState = level.getBlockState(pos);
        if (!existingState.isAir()) {
            return false;
        }
        return targetState.canSurvive(level, pos);
    }

    private static Direction[] shuffledDirections(ServerLevel level) {
        Direction[] shuffled = AXIS_DIRECTIONS.clone();
        for (int i = shuffled.length - 1; i > 0; i--) {
            int j = level.random.nextInt(i + 1);
            Direction temporary = shuffled[i];
            shuffled[i] = shuffled[j];
            shuffled[j] = temporary;
        }
        return shuffled;
    }

    private static Direction.Axis[] shuffledAxes(ServerLevel level) {
        Direction.Axis[] axes = new Direction.Axis[]{
                Direction.Axis.X,
                Direction.Axis.Y,
                Direction.Axis.Z
        };
        for (int i = axes.length - 1; i > 0; i--) {
            int j = level.random.nextInt(i + 1);
            Direction.Axis temporary = axes[i];
            axes[i] = axes[j];
            axes[j] = temporary;
        }
        return axes;
    }

    private static double sanitizeHeat(double impactHeatCelsius) {
        if (!Double.isFinite(impactHeatCelsius)) {
            return KrakkDamageApi.DEFAULT_IMPACT_HEAT_CELSIUS;
        }
        return impactHeatCelsius;
    }

    private static double parseFiniteDouble(JsonObject json, String key, double fallback) {
        if (!json.has(key)) {
            return fallback;
        }
        double value = GsonHelper.getAsDouble(json, key);
        if (!Double.isFinite(value)) {
            throw new IllegalArgumentException("'" + key + "' must be finite");
        }
        return value;
    }

    private static BlockState parseTargetState(JsonObject json) {
        String rawTargetId = GsonHelper.getAsString(json, "target");
        ResourceLocation targetId = ResourceLocation.tryParse(rawTargetId);
        if (targetId == null) {
            throw new IllegalArgumentException("invalid target block id '" + rawTargetId + "'");
        }
        Optional<Block> target = BuiltInRegistries.BLOCK.getOptional(targetId);
        Block targetBlock = target.orElseThrow(() -> new IllegalArgumentException("unknown target block '" + targetId + "'"));
        return targetBlock.defaultBlockState();
    }

    private static PlacementMode parsePlacementMode(JsonObject json) {
        String rawMode = GsonHelper.getAsString(json, "placement_mode", "random_adjacent");
        return switch (rawMode) {
            case "up_if_open" -> PlacementMode.UP_IF_OPEN;
            case "random_adjacent" -> PlacementMode.RANDOM_ADJACENT;
            case "random_open_axis" -> PlacementMode.RANDOM_OPEN_AXIS;
            default -> throw new IllegalArgumentException("unsupported placement_mode '" + rawMode + "'");
        };
    }

    private static ChanceModel parseChanceModel(JsonObject json, Double minHeat, Double maxHeat) {
        double chanceMin = parseFiniteDouble(json, "chance_min", 1.0D);
        double chanceMax = parseFiniteDouble(json, "chance_max", chanceMin);
        chanceMin = clampChance(chanceMin);
        chanceMax = clampChance(chanceMax);

        double defaultHeatMin = minHeat != null ? minHeat : KrakkDamageApi.DEFAULT_IMPACT_HEAT_CELSIUS;
        double defaultHeatMax = maxHeat != null ? maxHeat : defaultHeatMin;
        double chanceHeatMin = parseFiniteDouble(json, "chance_heat_min", defaultHeatMin);
        double chanceHeatMax = parseFiniteDouble(json, "chance_heat_max", defaultHeatMax);
        double chanceExponent = parseFiniteDouble(json, "chance_exponent", 1.0D);
        if (chanceExponent <= 0.0D) {
            chanceExponent = 1.0D;
        }
        return new ChanceModel(chanceMin, chanceMax, chanceHeatMin, chanceHeatMax, chanceExponent);
    }

    private static double clampChance(double chance) {
        if (!Double.isFinite(chance)) {
            return 0.0D;
        }
        return Math.max(0.0D, Math.min(1.0D, chance));
    }

    private static final class RuleReloadListener extends SimpleJsonResourceReloadListener {
        private RuleReloadListener() {
            super(GSON, RULE_DIRECTORY);
        }

        @Override
        protected void apply(Map<ResourceLocation, JsonElement> prepared, ResourceManager resourceManager, ProfilerFiller profiler) {
            List<ImpactPlacementRule> loadedRules = new ArrayList<>();
            for (Map.Entry<ResourceLocation, JsonElement> entry : prepared.entrySet()) {
                ResourceLocation fileId = entry.getKey();
                try {
                    JsonObject json = GsonHelper.convertToJsonObject(entry.getValue(), "impact placement");
                    BlockState targetState = parseTargetState(json);
                    PlacementMode placementMode = parsePlacementMode(json);
                    int priority = json.has("priority") ? GsonHelper.getAsInt(json, "priority") : 0;
                    Double minHeat = json.has("heat_celsius_min")
                            ? parseFiniteDouble(json, "heat_celsius_min", KrakkDamageApi.DEFAULT_IMPACT_HEAT_CELSIUS)
                            : null;
                    Double maxHeat = json.has("heat_celsius_max")
                            ? parseFiniteDouble(json, "heat_celsius_max", KrakkDamageApi.DEFAULT_IMPACT_HEAT_CELSIUS)
                            : null;
                    ChanceModel chanceModel = parseChanceModel(json, minHeat, maxHeat);
                    loadedRules.add(new ImpactPlacementRule(targetState, placementMode, priority, minHeat, maxHeat, chanceModel));
                } catch (Exception exception) {
                    LOGGER.warn("Skipped impact placement rule {}: {}", fileId, exception.getMessage());
                }
            }

            loadedRules.sort(Comparator.comparingInt(ImpactPlacementRule::priority).reversed());
            rules = List.copyOf(loadedRules);
        }
    }

    private enum PlacementMode {
        UP_IF_OPEN,
        RANDOM_ADJACENT,
        RANDOM_OPEN_AXIS
    }

    private record ImpactPlacementRule(BlockState targetState,
                                       PlacementMode placementMode,
                                       int priority,
                                       Double minHeatCelsius,
                                       Double maxHeatCelsius,
                                       ChanceModel chanceModel) {
        boolean matches(double impactHeatCelsius) {
            if (minHeatCelsius != null && impactHeatCelsius < minHeatCelsius) {
                return false;
            }
            if (maxHeatCelsius != null && impactHeatCelsius > maxHeatCelsius) {
                return false;
            }
            return true;
        }
    }

    private record ChanceModel(double chanceMin,
                               double chanceMax,
                               double chanceHeatMin,
                               double chanceHeatMax,
                               double chanceExponent) {
        double resolveChance(double impactHeatCelsius, double impactPower) {
            if (Math.abs(chanceMax - chanceMin) <= 1.0E-9D) {
                return clampChance(chanceMin);
            }
            if (Math.abs(chanceHeatMax - chanceHeatMin) <= 1.0E-9D) {
                return clampChance(chanceMax);
            }
            double normalized = (impactHeatCelsius - chanceHeatMin) / (chanceHeatMax - chanceHeatMin);
            normalized = Math.max(0.0D, Math.min(1.0D, normalized));
            if (Math.abs(chanceExponent - 1.0D) > 1.0E-6D) {
                normalized = Math.pow(normalized, chanceExponent);
            }
            double chance = chanceMin + ((chanceMax - chanceMin) * normalized);
            return clampChance(chance);
        }
    }
}
