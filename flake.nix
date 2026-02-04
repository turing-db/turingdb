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

          turingstdenv =
            if darwin
            then pkgs.llvmPackages_20.stdenv
            else pkgs.stdenv;
          
          darwinUtils  = pkgs.lib.optionals darwin [
            pkgs.llvmPackages_20.openmp
          ];

          sharedNativeBuildInputs = with pkgs; [
            cmake
            pkg-config
            git
            bison
            flex
          ];

          sharedBuildInputs = with pkgs; [
            curl
            openssl
            curl
            openssl
            boost
            openblas
            aws-sdk-cpp
            faiss
            zlib
          ] ++ lib.optionals darwin darwinUtils;

        in
        {
          devShells.default = pkgs.mkShell.override { stdenv = turingstdenv; } {
            nativeBuildInputs = sharedNativeBuildInputs;
            buildInputs = sharedBuildInputs;

            shellHook = "bash ./pull.sh";
          };
        }
      );
}
