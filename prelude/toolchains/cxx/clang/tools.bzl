# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is dual-licensed under either the MIT license found in the
# LICENSE-MIT file in the root directory of this source tree or the Apache
# License, Version 2.0 found in the LICENSE-APACHE file in the root directory
# of this source tree. You may select, at your option, one of the
# above-listed licenses.

load("@prelude//cxx:cxx_toolchain_types.bzl", "LinkerType")
load("@prelude//os_lookup:defs.bzl", "Os", "OsLookup")
load("@prelude//toolchains:cxx.bzl", "CxxToolsInfo")

def _path_clang_tools_impl(ctx) -> list[Provider]:
    target_os = ctx.attrs._target_os_type[OsLookup].os

    # On illumos, use g++ for C++ compilation to avoid libstdc++ compatibility issues
    if target_os == Os("illumos"):
        cxx_compiler = "g++"
        linker = "g++"
    else:
        cxx_compiler = "clang++"
        linker = "clang++"

    return [
        DefaultInfo(),
        CxxToolsInfo(
            compiler = "clang",
            compiler_type = "clang",
            cxx_compiler = cxx_compiler,
            asm_compiler = "clang",
            asm_compiler_type = "clang",
            rc_compiler = None,
            cvtres_compiler = None,
            archiver = "ar",
            archiver_type = "gnu",
            linker = linker,
            linker_type = LinkerType("gnu"),
        ),
    ]

path_clang_tools = rule(
    impl = _path_clang_tools_impl,
    attrs = {
        "_target_os_type": attrs.default_only(attrs.dep(providers = [OsLookup], default = "prelude//os_lookup/targets:os_lookup")),
    },
)
