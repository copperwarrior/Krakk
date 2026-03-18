#version 150

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

    float fogFactor = 0.0;
    if (FogEnd > FogStart) {
        fogFactor = clamp((vertexDistance - FogStart) / (FogEnd - FogStart), 0.0, 1.0);
    }
    // For this overlay path we fade crack color with fog instead of alpha,
    // matching CRUMBLING-style behavior more closely.
    color.rgb = mix(color.rgb, vec3(0.5), fogFactor);
    fragColor = color;
}
