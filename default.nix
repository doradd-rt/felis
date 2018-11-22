with import <nixpkgs> {};

stdenv.mkDerivation rec {
  name = "dolly";
  shellHook = ''
     unset CC CXX
     export CC=clang
     export CXX=clang++
   '';
   
  buildInputs = [
    jemalloc numactl llvm_6 clang_6 lldb_6 gtest gperftools python36 watchman nailgun
  ];
}