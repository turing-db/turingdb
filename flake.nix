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
          darwinUtils  = pkgs.lib.optionals pkgs.stdenv.isDarwin [
            pkgs.apple-sdk_14
            pkgs.libiconv
          ];

          sharedNativeBuildInputs = with pkgs; [
            cmake
            git
            bash

            curl
            openssl_3

            zlib

            bison
            flex

            openblas

            faiss
            aws-sdk-cpp
            gtest
          ];

          sharedBuildInputs = [] ++ darwinUtils;

        in
        {
          devShells.default = pkgs.mkShell {
            nativeBuildInputs = sharedNativeBuildInputs;
            buildInputs = sharedBuildInputs;

            shellHook = "bash ./pull.sh";
          };
        }
      );
}
