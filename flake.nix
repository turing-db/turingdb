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

          compiler = if darwin then pkgs.llvmPackages20 else pkgs.gcc15;

          turingstdenv = compiler.stdenv;
          
          darwinInputs = if darwin then [
            compiler.openmp
          ] else [];

          linuxInputs = if !darwin then [
            compiler
          ] else [];

          sharedNativeBuildInputs = with pkgs; [];

          sharedBuildInputs = with pkgs; [
            cmake
            git-lfs
            gnum4
          ] ++ darwinInputs ++ linuxInputs;

        in
        {
          packages.default = turingstdenv.mkDerivation {
            pname = "turingdb";
            version = "1.20";

            src = ./.;

            nativeBuildInputs = sharedNativeBuildInputs;
            buildInputs = sharedBuildInputs;

            cmakeFlags = [
              "-DNIX_BUILD=ON" # CMake flag for top-level turingdb CMakeLists
              "-DCMAKE_BUILD_TYPE=Release"
              "-DCMAKE_CXX_FLAGS=-fopenmp"
            ] ++ pkgs.lib.optionals darwin [
              "-DOpenMP_CXX_FLAGS=-fopenmp"
              "-DOpenMP_CXX_LIB_NAMES=omp"
              "-DOpenMP_omp_LIBRARY=${pkgs.llvmPackages_20.openmp}/lib/libomp.dylib"
            ];

            preConfigure = ''
              if [ ! -d "common/.git" ]; then
                echo "Warning: Submodules might not be properly initialized"
              fi
            '';

            meta = {
              description = "High performance in-memory column-oriented graph database engine";
              longDescription = ''
                TuringDB is a high-performance in-memory column-oriented graph database engine
                designed for analytical and read-intensive workloads. Built from scratch in C++,
                it delivers millisecond query latency on graphs with millions of nodes and edges.

                Key features:
                - 0.1-50ms query latency for analytical queries on 10M+ node graphs
                - Zero-lock concurrency model
                - Git-like versioning system for graphs
                - OpenCypher query language support
                - Python SDK with comprehensive API
              '';
              homepage = "https://turingdb.ai";
              changelog = "https://github.com/turing-db/turingdb/releases";
              # license = pkgs.licenses.bsl11;
              platforms = [ "x86_64-linux" "aarch64-darwin" ];
              mainProgram = "turingdb";
              maintainers = with pkgs.lib.maintainers; [ cyrusknopf ];
            };
          };

          # Dev shell
          devShells.default = pkgs.mkShell.override { stdenv = turingstdenv; } {
            nativeBuildInputs = sharedNativeBuildInputs;
            buildInputs = sharedBuildInputs;

            # shellHook = "bash ./pull.sh";
          };
        }
      );
}
