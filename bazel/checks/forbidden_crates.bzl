"""`forbidden_crates_test`: fails if a crate links any crate whose name starts with a forbidden prefix.

Reads the crate's transitive dependencies from rules_rust's `DepInfo`, the same set
rustc is given, so the check compiles nothing and sees exactly the configured graph.
"""

load("@rules_rust//rust:defs.bzl", "rust_common")

def _forbidden_crates_test_impl(ctx):
    dep_info = ctx.attr.crate[rust_common.dep_info]
    crates = sorted({
        "{} ({})".format(c.name, c.owner): None
        for c in dep_info.transitive_crates.to_list()
    }.keys())
    hits = [c for c in crates if any([c.startswith(p) for p in ctx.attr.forbidden])]

    if hits:
        lines = ["echo 'error: {} depends on forbidden crates (prefixes: {}):' >&2".format(
            ctx.attr.crate.label,
            ", ".join(ctx.attr.forbidden),
        )]
        lines += ["echo '  {}' >&2".format(h) for h in hits]
        lines.append("exit 1")
    elif not crates:
        # A wrong label or provider would otherwise pass silently.
        lines = ["echo 'error: {} has no dependencies; the check is broken' >&2".format(ctx.attr.crate.label), "exit 1"]
    else:
        lines = ["echo '{}: {} crates, none forbidden'".format(ctx.attr.crate.label, len(crates))]

    script = ctx.actions.declare_file(ctx.label.name + ".sh")
    ctx.actions.write(script, "#!/bin/sh\n" + "\n".join(lines) + "\n", is_executable = True)
    return [DefaultInfo(executable = script)]

forbidden_crates_test = rule(
    implementation = _forbidden_crates_test_impl,
    attrs = {
        "crate": attr.label(mandatory = True, providers = [rust_common.dep_info]),
        "forbidden": attr.string_list(
            mandatory = True,
            doc = "Crate-name prefixes (underscore form, as rustc sees them).",
        ),
    },
    test = True,
)
