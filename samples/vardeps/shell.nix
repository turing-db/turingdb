{ pkgs ? import <nixpkgs> {} }:

pkgs.mkShell {
  packages = [ pkgs.nodejs pkgs.python3 ];

  shellHook = ''
    if [ ! -d ${toString ./.}/node_modules ]; then
      npm --prefix ${toString ./.} install --silent
    fi
  '';
}
