{ pkgs ? import <nixpkgs> {} }:

pkgs.mkShell {
  buildInputs = [
    pkgs.llvmPackages.mlir
    pkgs.llvmPackages.llvm
    pkgs.cmake
    pkgs.ninja
  ];

  shellHook = ''
    export MLIR_DIR=${pkgs.llvmPackages.mlir}/lib/cmake/mlir
    export LLVM_DIR=${pkgs.llvmPackages.llvm}/lib/cmake/llvm
  '';
}
