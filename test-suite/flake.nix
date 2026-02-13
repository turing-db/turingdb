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
          compiler = if darwin then pkgs.llvmPackages20 else pkgs.gcc15;
          turingstdenv = compiler.stdenv;
          
          sharedNativeBuildInputs = with pkgs; [];

          sharedBuildInputs = with pkgs; [
            bun
          ];

        in
        {
          # Dev shell
          devShells.default = pkgs.mkShell.override { stdenv = turingstdenv; } {
            nativeBuildInputs = sharedNativeBuildInputs;
            buildInputs = sharedBuildInputs;

            # shellHook = "bash ./pull.sh";
          };
        }
      );
}
