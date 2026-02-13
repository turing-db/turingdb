{
  description = "TuringDB - the fastest in-memory graph database";

  inputs = {
    nixpkgs.url =  "github:nixos/nixpkgs/nixpkgs-25.05-darwin";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachSystem [
      "x86_64-linux"
      "aarch64-darwin"
    ] (system:
        let
          pkgs = nixpkgs.legacyPackages.${system};

          darwin = pkgs.stdenv.isDarwin;

          # We use LLVM on macOS; gcc on Linux
          compiler = if darwin then pkgs.llvmPackages_20 else pkgs.gcc15;
          turingstdenv = compiler.stdenv;
          
          # If using LLVM, we need to specify OpenMP explictly
          darwinInputs = with pkgs; if darwin then [
            compiler.openmp
            compiler.libcxx

            # macOS-specific debugging utilities
            lldb_20
          ] else [];

          # If on Linux, add gcc15 as an input explicitly
          linuxInputs = with pkgs; if !darwin then [
            compiler

            # Linux-specific debugging utilities
            gdb
            valgrind

            # Linux-specific performance analysis tools
            linuxPackages_latest.perf
          ] else [];

          sharedNativeBuildInputs = with pkgs; [];

          sharedBuildInputs = with pkgs; [
            # Build tools
            cmake
            git-lfs
            gnum4

            # clangd, clang-format, etc.
            clang-tools
          ] ++ darwinInputs ++ linuxInputs;

          macos_setup = ''
            # Detect the macOS SDK path
            MACOS_SDK_PATH=$(xcrun --show-sdk-path 2>/dev/null)
          
            # Set LLVM_PREFIX to the Nix store path
            LLVM_PREFIX="${compiler.clang}"
            LIBCXX_PATH="${compiler.libcxx}/lib/c++"
          
            # Common macOS toolchain args for building all dependencies with LLVM
            MACOS_COMPILER_ARGS=(
                "-DCMAKE_C_COMPILER=$LLVM_PREFIX/bin/clang"
                "-DCMAKE_CXX_COMPILER=$LLVM_PREFIX/bin/clang++"
                "-DCMAKE_CXX_FLAGS=-stdlib=libc++"
                "-DCMAKE_OSX_SYSROOT=$MACOS_SDK_PATH"
                "-DCMAKE_EXE_LINKER_FLAGS=-L$LIBCXX_PATH -Wl,-rpath,$LIBCXX_PATH"
                "-DCMAKE_SHARED_LINKER_FLAGS=-L$LIBCXX_PATH -Wl,-rpath,$LIBCXX_PATH"
            )
          
            # Build a properly quoted CMAKE_ARGS string
            QUOTED_ARGS=()
            for arg in "''${MACOS_COMPILER_ARGS[@]}"; do
                QUOTED_ARGS+=("'$arg'")
            done
          
            # Write environment variables to $MACOS_SETENV
            DEPENDENCIES_DIR=external/dependencies
            MACOS_SETENV=$DEPENDENCIES_DIR/macos_setenv.sh
            echo "export LLVM_PREFIX=$LLVM_PREFIX" > "$MACOS_SETENV"
            echo "export CMAKE_ARGS=\"''${QUOTED_ARGS[*]}\"" >> "$MACOS_SETENV"
          
            echo "LLVM_PREFIX: $LLVM_PREFIX"
            echo "LIBCXX_PATH: $LIBCXX_PATH"
            echo "macOS SDK: $MACOS_SDK_PATH"
            echo ""
            echo "Environment written to: $MACOS_SETENV"
            echo "Source it with: source $MACOS_SETENV"
            echo ""
            echo "libc++ static libraries available at:"
            ls -lh ${compiler.libcxx}/lib/*.a 2>/dev/null || echo "  (none found)"
          '';
        in
        {
          # Development shell: facilitates manual building and development of TuringDB
          devShells.default = pkgs.mkShell.override { stdenv = turingstdenv; } {
            nativeBuildInputs = sharedNativeBuildInputs;
            buildInputs = sharedBuildInputs;

            shellHook = if darwin then macos_setup else "echo 'linux'";
          };
        }
      );
}
