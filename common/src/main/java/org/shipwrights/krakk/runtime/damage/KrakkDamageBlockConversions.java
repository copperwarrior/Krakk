package org.shipwrights.krakk.runtime.damage;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mojang.logging.LogUtils;
import dev.architectury.registry.ReloadListenerRegistry;
import net.minecraft.core.BlockPos;
import net.minecraft.core.registries.BuiltInRegistries;
import net.minecraft.core.registries.Registries;
import net.minecraft.resources.ResourceLocation;
import net.minecraft.server.level.ServerLevel;
import net.minecraft.server.packs.PackType;
import net.minecraft.server.packs.resources.ResourceManager;
import net.minecraft.server.packs.resources.SimpleJsonResourceReloadListener;
import net.minecraft.tags.TagKey;
import net.minecraft.util.GsonHelper;
import net.minecraft.util.profiling.ProfilerFiller;
import net.minecraft.world.level.block.Block;
import net.minecraft.world.level.block.state.BlockState;
import net.minecraft.world.level.block.state.properties.Property;
import org.shipwrights.krakk.api.damage.KrakkDamageApi;
import org.shipwrights.krakk.engine.damage.KrakkDamageCurves;
import org.shipwrights.krakk.Krakk;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public final class KrakkDamageBlockConversions {
    private static final Logger LOGGER = LogUtils.getLogger();
    private static final Gson GSON = new GsonBuilder().disableHtmlEscaping().create();
    private static final String RULE_DIRECTORY = "krakk_damage_block_conversions";
    private static final ResourceLocation RELOAD_LISTENER_ID = new ResourceLocation(Krakk.MOD_ID, RULE_DIRECTORY);
    private static final int MAX_CASCADE_STEPS = 16;
    private static final RuleReloadListener RELOAD_LISTENER = new RuleReloadListener();

    private static volatile Map<Block, List<DamageConversionRule>> rulesByDirectSource = Map.of();
    private static volatile List<DamageConversionRule> dynamicRules = List.of();
    private static boolean initialized;

    private KrakkDamageBlockConversions() {
    }

    public static void init() {
        if (initialized) {
            return;
        }
        initialized = true;
        ReloadListenerRegistry.register(PackType.SERVER_DATA, RELOAD_LISTENER, RELOAD_LISTENER_ID);
    }

    public static boolean hasConversionRule(BlockState state) {
        if (state == null || state.isAir()) return false;
        if (rulesByDirectSource.containsKey(state.getBlock())) return true;
        for (DamageConversionRule rule : dynamicRules) {
            if (rule.sourceSelector().matches(state)) return true;
        }
        return false;
    }

    public static boolean applyConversionForDamageState(ServerLevel level, BlockPos blockPos, BlockState blockState,
                                                        int damageState, double impactPower, double impactHeatCelsius) {
        if (level == null || blockPos == null || blockState == null || blockState.isAir()) {
            return false;
        }
        int clampedDamageState = clampDamageState(damageState);
        double sanitizedHeat = sanitizeHeat(impactHeatCelsius);
        BlockState currentState = blockState;
        boolean converted = false;
        Set<BlockState> seenStates = new HashSet<>();
        seenStates.add(currentState);

        for (int i = 0; i < MAX_CASCADE_STEPS; i++) {
            if (!applySingleConversionStep(level, blockPos, currentState, clampedDamageState, impactPower, sanitizedHeat)) {
                break;
            }
            converted = true;
            currentState = level.getBlockState(blockPos);
            if (currentState.isAir()) {
                break;
            }
            if (!seenStates.add(currentState)) {
                break;
            }
        }
        return converted;
    }

    private static boolean applySingleConversionStep(ServerLevel level, BlockPos blockPos, BlockState sourceState,
                                                     int damageState, double impactPower, double impactHeatCelsius) {
        DamageConversionRule rule = evaluateRules(sourceState, damageState, impactPower, impactHeatCelsius);
        if (rule == null) {
            return false;
        }

        BlockState targetState = resolveTargetState(sourceState, rule);
        if (targetState == null || targetState == sourceState || targetState.equals(sourceState)) {
            return false;
        }
        return level.setBlock(blockPos, targetState, 3);
    }

    private static DamageConversionRule evaluateRules(BlockState state, int damageState,
                                                      double impactPower, double impactHeatCelsius) {
        List<DamageConversionRule> direct = rulesByDirectSource.get(state.getBlock());
        if (direct != null) {
            for (DamageConversionRule rule : direct) {
                if (rule.matches(state, damageState, impactPower, impactHeatCelsius)) {
                    return rule;
                }
            }
        }
        for (DamageConversionRule rule : dynamicRules) {
            if (rule.matches(state, damageState, impactPower, impactHeatCelsius)) {
                return rule;
            }
        }
        return null;
    }

    private static BlockState resolveTargetState(BlockState sourceState, DamageConversionRule rule) {
        BlockState targetState = null;
        if (rule.target() != null) {
            targetState = rule.target().defaultBlockState();
        } else if (rule.targetPrefixStrip() != null && !rule.targetPrefixStrip().isEmpty()) {
            targetState = resolvePrefixedTargetState(sourceState, rule.targetPrefixStrip());
        } else if (!rule.targetProperties().isEmpty()) {
            targetState = sourceState;
        }

        if (targetState == null) {
            return null;
        }
        targetState = copyCompatibleProperties(sourceState, targetState);
        targetState = applyConfiguredProperties(targetState, rule.targetProperties());
        return targetState;
    }

    private static BlockState resolvePrefixedTargetState(BlockState sourceState, String stripPrefix) {
        ResourceLocation sourceId = BuiltInRegistries.BLOCK.getKey(sourceState.getBlock());
        if (sourceId == null) {
            return null;
        }
        String sourcePath = sourceId.getPath();
        if (!sourcePath.startsWith(stripPrefix)) {
            return null;
        }
        String targetPath = sourcePath.substring(stripPrefix.length());
        if (targetPath.isEmpty()) {
            return null;
        }
        ResourceLocation targetId = new ResourceLocation(sourceId.getNamespace(), targetPath);
        Optional<Block> targetBlock = BuiltInRegistries.BLOCK.getOptional(targetId);
        return targetBlock.map(block -> block.defaultBlockState()).orElse(null);
    }

    private static BlockState copyCompatibleProperties(BlockState sourceState, BlockState targetState) {
        BlockState convertedState = targetState;
        for (Property<?> sourceProperty : sourceState.getProperties()) {
            Property<?> targetProperty = targetState.getBlock().getStateDefinition().getProperty(sourceProperty.getName());
            if (targetProperty == null) {
                continue;
            }

            String serializedValue = getSerializedPropertyValue(sourceState, sourceProperty);
            convertedState = applySerializedPropertyValue(convertedState, targetProperty, serializedValue);
        }
        return convertedState;
    }

    private static BlockState applyConfiguredProperties(BlockState state, Map<String, String> properties) {
        BlockState configuredState = state;
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            Property<?> property = configuredState.getBlock().getStateDefinition().getProperty(entry.getKey());
            if (property == null) {
                continue;
            }
            configuredState = applySerializedPropertyValue(configuredState, property, entry.getValue());
        }
        return configuredState;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static String getSerializedPropertyValue(BlockState sourceState, Property<?> sourceProperty) {
        Property rawSourceProperty = sourceProperty;
        Comparable rawValue = (Comparable) sourceState.getValue(rawSourceProperty);
        return rawSourceProperty.getName(rawValue);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static BlockState applySerializedPropertyValue(BlockState state, Property<?> targetProperty, String serializedValue) {
        Property rawTargetProperty = targetProperty;
        Optional<?> parsedValue = rawTargetProperty.getValue(serializedValue);
        if (parsedValue.isEmpty()) {
            return state;
        }
        return state.setValue(rawTargetProperty, (Comparable) parsedValue.get());
    }

    private static int clampDamageState(int value) {
        return Math.max(0, Math.min(KrakkDamageCurves.MAX_DAMAGE_STATE, value));
    }

    private static double sanitizeHeat(double impactHeatCelsius) {
        if (!Double.isFinite(impactHeatCelsius)) {
            return KrakkDamageApi.DEFAULT_IMPACT_HEAT_CELSIUS;
        }
        return impactHeatCelsius;
    }

    private static int parseThreshold(JsonObject json) {
        if (json.has("damage_state")) {
            return clampDamageState(GsonHelper.getAsInt(json, "damage_state"));
        }
        if (json.has("damage_fraction")) {
            double fraction = GsonHelper.getAsDouble(json, "damage_fraction");
            if (!Double.isFinite(fraction) || fraction < 0.0D) {
                throw new IllegalArgumentException("'damage_fraction' must be a finite value >= 0");
            }
            int threshold = (int) Math.ceil(fraction * KrakkDamageCurves.MAX_DAMAGE_STATE);
            return clampDamageState(threshold);
        }
        return 0;
    }

    private static Double parseFiniteDouble(JsonObject json, String key) {
        if (!json.has(key)) {
            return null;
        }
        double value = GsonHelper.getAsDouble(json, key);
        if (!Double.isFinite(value)) {
            throw new IllegalArgumentException("'" + key + "' must be finite");
        }
        return value;
    }

    private static Block parseBlock(JsonObject json, String key) {
        if (!json.has(key)) {
            return null;
        }
        String rawId = GsonHelper.getAsString(json, key);
        ResourceLocation parsed = ResourceLocation.tryParse(rawId);
        if (parsed == null) {
            throw new IllegalArgumentException("invalid block id '" + rawId + "'");
        }
        return BuiltInRegistries.BLOCK.getOptional(parsed)
                .orElseThrow(() -> new IllegalArgumentException("unknown block '" + parsed + "'"));
    }

    private static SourceSelector parseSourceSelector(JsonObject json) {
        Block sourceBlock = parseBlock(json, "source");
        if (sourceBlock != null) {
            return new SourceSelector(sourceBlock, null, null);
        }

        if (json.has("source_tag")) {
            String rawTag = GsonHelper.getAsString(json, "source_tag");
            ResourceLocation tagId = ResourceLocation.tryParse(rawTag);
            if (tagId == null) {
                throw new IllegalArgumentException("invalid source tag '" + rawTag + "'");
            }
            return new SourceSelector(null, TagKey.create(Registries.BLOCK, tagId), null);
        }

        if (json.has("source_prefix")) {
            String prefix = GsonHelper.getAsString(json, "source_prefix");
            if (prefix.isEmpty()) {
                throw new IllegalArgumentException("source_prefix cannot be empty");
            }
            return new SourceSelector(null, null, prefix);
        }

        throw new IllegalArgumentException("missing source selector: expected one of source, source_tag, source_prefix");
    }

    private static Map<String, String> parseTargetProperties(JsonObject json) {
        if (!json.has("target_properties")) {
            return Map.of();
        }
        JsonObject object = GsonHelper.getAsJsonObject(json, "target_properties");
        if (object.entrySet().isEmpty()) {
            return Map.of();
        }
        Map<String, String> properties = new HashMap<>();
        for (Map.Entry<String, JsonElement> entry : object.entrySet()) {
            JsonElement element = entry.getValue();
            if (!element.isJsonPrimitive()) {
                continue;
            }
            properties.put(entry.getKey(), element.getAsJsonPrimitive().getAsString());
        }
        return Map.copyOf(properties);
    }

    private static Map<String, Set<String>> parseSourceProperties(JsonObject json) {
        if (!json.has("source_properties")) {
            return Map.of();
        }
        JsonObject object = GsonHelper.getAsJsonObject(json, "source_properties");
        if (object.entrySet().isEmpty()) {
            return Map.of();
        }
        Map<String, Set<String>> properties = new HashMap<>();
        for (Map.Entry<String, JsonElement> entry : object.entrySet()) {
            Set<String> values = parseAllowedSourcePropertyValues(entry.getValue());
            if (!values.isEmpty()) {
                properties.put(entry.getKey(), values);
            }
        }
        return Map.copyOf(properties);
    }

    private static Set<String> parseAllowedSourcePropertyValues(JsonElement element) {
        if (element == null || element.isJsonNull()) {
            return Set.of();
        }
        if (element.isJsonPrimitive()) {
            return Set.of(element.getAsJsonPrimitive().getAsString());
        }
        if (!element.isJsonArray()) {
            return Set.of();
        }

        JsonArray array = element.getAsJsonArray();
        if (array.isEmpty()) {
            return Set.of();
        }

        Set<String> values = new LinkedHashSet<>();
        for (JsonElement value : array) {
            if (value.isJsonPrimitive()) {
                values.add(value.getAsJsonPrimitive().getAsString());
            }
        }
        return values.isEmpty() ? Set.of() : Set.copyOf(values);
    }

    private static boolean sourcePropertiesMatch(BlockState state, Map<String, Set<String>> sourceProperties) {
        if (sourceProperties.isEmpty()) {
            return true;
        }
        for (Map.Entry<String, Set<String>> entry : sourceProperties.entrySet()) {
            Property<?> property = state.getBlock().getStateDefinition().getProperty(entry.getKey());
            if (property == null) {
                return false;
            }
            String serializedValue = getSerializedPropertyValue(state, property);
            if (!entry.getValue().contains(serializedValue)) {
                return false;
            }
        }
        return true;
    }

    private static final class RuleReloadListener extends SimpleJsonResourceReloadListener {
        private RuleReloadListener() {
            super(GSON, RULE_DIRECTORY);
        }

        @Override
        protected void apply(Map<ResourceLocation, JsonElement> prepared, ResourceManager resourceManager, ProfilerFiller profiler) {
            Map<Block, List<DamageConversionRule>> loadedDirectRules = new HashMap<>();
            List<DamageConversionRule> loadedDynamicRules = new ArrayList<>();

            for (Map.Entry<ResourceLocation, JsonElement> entry : prepared.entrySet()) {
                ResourceLocation fileId = entry.getKey();
                try {
                    JsonObject json = GsonHelper.convertToJsonObject(entry.getValue(), "damage conversion");
                    SourceSelector sourceSelector = parseSourceSelector(json);
                    Block target = parseBlock(json, "target");
                    String targetPrefixStrip = json.has("target_prefix_strip")
                            ? GsonHelper.getAsString(json, "target_prefix_strip")
                            : null;
                    Map<String, Set<String>> sourceProperties = parseSourceProperties(json);
                    Map<String, String> targetProperties = parseTargetProperties(json);

                    if (target == null && (targetPrefixStrip == null || targetPrefixStrip.isEmpty()) && targetProperties.isEmpty()) {
                        throw new IllegalArgumentException("missing target definition");
                    }

                    int threshold = parseThreshold(json);
                    Double exactHeat = parseFiniteDouble(json, "heat_celsius");
                    Double minHeat = parseFiniteDouble(json, "heat_celsius_min");
                    Double maxHeat = parseFiniteDouble(json, "heat_celsius_max");
                    if (exactHeat != null) {
                        minHeat = exactHeat;
                        maxHeat = exactHeat;
                    }
                    int priority = json.has("priority") ? GsonHelper.getAsInt(json, "priority") : 0;

                    DamageConversionRule rule = new DamageConversionRule(
                            sourceSelector,
                            threshold,
                            minHeat,
                            maxHeat,
                            target,
                            targetPrefixStrip,
                            sourceProperties,
                            targetProperties,
                            priority
                    );

                    if (sourceSelector.sourceBlock() != null) {
                        loadedDirectRules.computeIfAbsent(sourceSelector.sourceBlock(), key -> new ArrayList<>()).add(rule);
                    } else {
                        loadedDynamicRules.add(rule);
                    }
                } catch (Exception exception) {
                    LOGGER.warn("Skipped damage conversion rule {}: {}", fileId, exception.getMessage());
                }
            }

            Comparator<DamageConversionRule> comparator = Comparator
                    .comparingInt(DamageConversionRule::priority).reversed()
                    .thenComparing(Comparator.comparingInt(DamageConversionRule::minDamageState).reversed());

            Map<Block, List<DamageConversionRule>> immutableDirectRules = new HashMap<>();
            for (Map.Entry<Block, List<DamageConversionRule>> entry : loadedDirectRules.entrySet()) {
                List<DamageConversionRule> sortedRules = new ArrayList<>(entry.getValue());
                sortedRules.sort(comparator);
                immutableDirectRules.put(entry.getKey(), List.copyOf(sortedRules));
            }

            loadedDynamicRules.sort(comparator);
            rulesByDirectSource = Map.copyOf(immutableDirectRules);
            dynamicRules = List.copyOf(loadedDynamicRules);
        }
    }

    private record SourceSelector(Block sourceBlock, TagKey<Block> sourceTag, String sourcePrefix) {
        boolean matches(BlockState state) {
            if (state == null || state.isAir()) {
                return false;
            }
            if (sourceBlock != null) {
                return state.is(sourceBlock);
            }
            if (sourceTag != null) {
                return state.is(sourceTag);
            }
            if (sourcePrefix != null) {
                ResourceLocation blockId = BuiltInRegistries.BLOCK.getKey(state.getBlock());
                if (blockId == null) {
                    return false;
                }
                return blockId.getPath().startsWith(sourcePrefix);
            }
            return false;
        }
    }

    private record DamageConversionRule(SourceSelector sourceSelector,
                                        int minDamageState,
                                        Double minHeatCelsius,
                                        Double maxHeatCelsius,
                                        Block target,
                                        String targetPrefixStrip,
                                        Map<String, Set<String>> sourceProperties,
                                        Map<String, String> targetProperties,
                                        int priority) {
        boolean matches(BlockState state, int damageState, double impactPower, double impactHeatCelsius) {
            if (!sourceSelector.matches(state)) {
                return false;
            }
            if (!sourcePropertiesMatch(state, sourceProperties)) {
                return false;
            }
            if (damageState < minDamageState) {
                return false;
            }
            if (minHeatCelsius != null && impactHeatCelsius < minHeatCelsius) {
                return false;
            }
            if (maxHeatCelsius != null && impactHeatCelsius > maxHeatCelsius) {
                return false;
            }
            return true;
        }
    }
}
