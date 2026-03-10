package org.shipwrights.krakk.engine.damage;

public final class KrakkDamageDecay {
    private KrakkDamageDecay() {
    }

    public static DecayResult applyDecay(int currentState, long elapsedTicks, long decayIntervalTicks) {
        if (currentState <= 0) {
            return new DecayResult(0, 0L, false);
        }
        if (elapsedTicks < decayIntervalTicks) {
            return new DecayResult(currentState, 0L, false);
        }

        int decayAmount = (int) (elapsedTicks / decayIntervalTicks);
        int decayedState = Math.max(0, currentState - decayAmount);
        long consumedTicks = (long) decayAmount * decayIntervalTicks;
        return new DecayResult(decayedState, consumedTicks, decayedState != currentState);
    }

    public record DecayResult(int state, long consumedTicks, boolean changed) {
    }
}
