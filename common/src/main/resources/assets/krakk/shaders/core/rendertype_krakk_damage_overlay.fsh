#version 150

#moj_import <fog.glsl>

uniform sampler2D Sampler0;

uniform vec4 ColorModulator;
uniform float FogStart;
uniform float FogEnd;
uniform vec4 FogColor;

in float vertexDistance;
in vec4 vertexColor;
in vec2 texCoord0;

out vec4 fragColor;

void main() {
    vec4 color = texture(Sampler0, texCoord0) * vertexColor * ColorModulator;
    if (color.a < 0.1) {
        discard;
    }

    // Keep vanilla-style fog color blending, but also fade crack alpha by fog factor
    // so distant overlays do not stay fully opaque under loader/shader variants.
    float fogFactor = 0.0;
    if (FogEnd > FogStart) {
        fogFactor = clamp((vertexDistance - FogStart) / (FogEnd - FogStart), 0.0, 1.0);
    }
    color = linear_fog(color, vertexDistance, FogStart, FogEnd, FogColor);
    color.a *= (1.0 - fogFactor);
    if (color.a <= 0.001) {
        discard;
    }
    fragColor = color;
}
