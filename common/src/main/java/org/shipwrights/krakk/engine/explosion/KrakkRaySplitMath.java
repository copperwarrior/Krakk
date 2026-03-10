package org.shipwrights.krakk.engine.explosion;

public final class KrakkRaySplitMath {
    private KrakkRaySplitMath() {
    }

    public static boolean shouldSplitRay(int splitDepth, int maxSplitDepth, double energy, double minSplitEnergy,
                                         double x, double y, double z,
                                         double centerX, double centerY, double centerZ,
                                         double baseAngularSpacingSquared, double splitDistanceThresholdSquared) {
        if (splitDepth >= maxSplitDepth || energy <= minSplitEnergy) {
            return false;
        }

        double fromCenterX = x - centerX;
        double fromCenterY = y - centerY;
        double fromCenterZ = z - centerZ;
        double radialDistanceSquared =
                (fromCenterX * fromCenterX) + (fromCenterY * fromCenterY) + (fromCenterZ * fromCenterZ);
        if (radialDistanceSquared <= 1.0E-12D) {
            return false;
        }

        double effectiveAngularSpacingSquared = baseAngularSpacingSquared / (double) (1 << splitDepth);
        return radialDistanceSquared * effectiveAngularSpacingSquared >= splitDistanceThresholdSquared;
    }

    public static Vec3 perpendicularAxis(double x, double y, double z) {
        double ax;
        double ay;
        double az;
        if (Math.abs(y) < 0.99D) {
            // cross(dir, up)
            ax = -z;
            ay = 0.0D;
            az = x;
        } else {
            // cross(dir, +X)
            ax = 0.0D;
            ay = z;
            az = -y;
        }
        double length = Math.sqrt((ax * ax) + (ay * ay) + (az * az));
        if (length <= 1.0E-8D) {
            return new Vec3(1.0D, 0.0D, 0.0D);
        }
        return new Vec3(ax / length, ay / length, az / length);
    }

    public static Vec3 rotateAroundAxis(double vx, double vy, double vz, Vec3 axis, double angleRadians) {
        double cos = Math.cos(angleRadians);
        double sin = Math.sin(angleRadians);
        double dot = (axis.x * vx) + (axis.y * vy) + (axis.z * vz);

        double crossX = (axis.y * vz) - (axis.z * vy);
        double crossY = (axis.z * vx) - (axis.x * vz);
        double crossZ = (axis.x * vy) - (axis.y * vx);

        double rx = (vx * cos) + (crossX * sin) + (axis.x * dot * (1.0D - cos));
        double ry = (vy * cos) + (crossY * sin) + (axis.y * dot * (1.0D - cos));
        double rz = (vz * cos) + (crossZ * sin) + (axis.z * dot * (1.0D - cos));

        double length = Math.sqrt((rx * rx) + (ry * ry) + (rz * rz));
        if (length <= 1.0E-8D) {
            return new Vec3(vx, vy, vz);
        }
        return new Vec3(rx / length, ry / length, rz / length);
    }

    public record Vec3(double x, double y, double z) {
    }
}
