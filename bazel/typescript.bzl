"""Shared definitions for the TypeScript SDK's BUILD files."""

# The platforms the Bazel build supports (`platform_triples` in MODULE.bazel), as
# `@napi-rs/cli` names them: the `<platform>` in `durable.<platform>.node`, which
# `platformTriple()` in durable-workflow-ts/ts/native.ts computes at run time. Each maps
# to the constraints of the target platform; `//bazel:<triple>` is a config_setting.
NAPI_TRIPLES = {
    "linux-x64-gnu": ["@platforms//os:linux", "@platforms//cpu:x86_64"],
    "linux-arm64-gnu": ["@platforms//os:linux", "@platforms//cpu:aarch64"],
    "darwin-arm64": ["@platforms//os:macos", "@platforms//cpu:aarch64"],
}
