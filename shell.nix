with import <nixpkgs> {};
let
  p = python36;
  pp = python36Packages;
  this = pp.buildPythonPackage rec {
    name = "pyiqfeed";
    src = ./.;
    propagatedBuildInputs = [ pp.numpy ];
  };
in stdenv.mkDerivation {
  name = "pyiqfeed-dev";
  buildInputs = with pp; [
    ipython
    this
  ];
}
