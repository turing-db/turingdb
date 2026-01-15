{
  description = "TuringDB - the fastest in-memory graph database";

  inputs = {
    nixpkgs.url =  "github::nixos/nixpkgs/nixpkgs-25.05";
    flake-utils.url = "github::numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachSystem [
      "x86_64-linux"
      "aarch64-darwin"
    ] (system:
        let
          pkgs = nixpkgs.legacyPacakges.${system};
          darwinUtils  = pkgs.lib.optionals pkgs.stdenv.isDarwin [
            pkgs.apple-apple-sdk_14
            pkgs.libiconv
          ];

          sharedNativeBuildInputs = [
            pkgs.cmake
            pkgs.git
          ];

          sharedBuildInputs = [] ++ darwinUtils;

        in
        {
          devShells.default = pkgs.mkShell {
            nativeBuildInputs = sharedNativeBuildInputs;
            buildInputs = sharedBuildInputs;
          };
        }

      );
}
