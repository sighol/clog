{
  description = "A pretty-printer for JSONL logs";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    crane.url = "github:ipetkov/crane";
  };

  outputs = { self, nixpkgs, flake-utils, crane, ... }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs {
          inherit system;
        };

        craneLib = crane.mkLib pkgs;

        # Filter sources to only include relevant files (ignores target/, git files, etc. to optimize rebuilds)
        src = craneLib.cleanCargoSource ./.;

        commonArgs = {
          inherit src;
          strictDeps = true;
        };

        # Build dependency artifacts separately to cache them effectively
        cargoArtifacts = craneLib.buildDepsOnly commonArgs;

        # Build the actual crate
        clog = craneLib.buildPackage (commonArgs // {
          inherit cargoArtifacts;
        });
      in
      {
        packages = {
          default = clog;
          inherit clog;
        };

        apps.default = flake-utils.lib.mkApp {
          drv = clog;
        };

        devShells.default = pkgs.mkShell {
          inputsFrom = [ clog ];
          packages = with pkgs; [
            cargo
            rustc
            clippy
            rustfmt
            rust-analyzer
          ];
        };
      }
    );
}
