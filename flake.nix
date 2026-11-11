{
  description = "Deterministic investment-research data, modeling, and reporting workflows";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-unstable";
    pyproject-nix = {
      url = "github:pyproject-nix/pyproject.nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    uv2nix = {
      url = "github:pyproject-nix/uv2nix";
      inputs.nixpkgs.follows = "nixpkgs";
      inputs.pyproject-nix.follows = "pyproject-nix";
    };
    pyproject-build-systems = {
      url = "github:pyproject-nix/build-system-pkgs";
      inputs.nixpkgs.follows = "nixpkgs";
      inputs.pyproject-nix.follows = "pyproject-nix";
      inputs.uv2nix.follows = "uv2nix";
    };
  };

  outputs =
    {
      nixpkgs,
      pyproject-nix,
      uv2nix,
      pyproject-build-systems,
      ...
    }:
    let
      inherit (nixpkgs) lib;
      systems = [
        "aarch64-darwin"
        "x86_64-darwin"
        "aarch64-linux"
        "x86_64-linux"
      ];
      # Dependencies come from uv.lock, so the Nix build installs exactly what
      # `uv sync` does.
      workspace = uv2nix.lib.workspace.loadWorkspace { workspaceRoot = ./.; };
      mkPackage =
        pkgs:
        let
          pythonSet =
            (pkgs.callPackage pyproject-nix.build.packages {
              python = pkgs.python3;
            }).overrideScope
              (
                lib.composeManyExtensions [
                  pyproject-build-systems.overlays.wheel
                  (workspace.mkPyprojectOverlay { sourcePreference = "wheel"; })
                ]
              );
          inherit (pkgs.callPackage pyproject-nix.build.util { }) mkApplication;
        in
        (mkApplication {
          venv = pythonSet.mkVirtualEnv "finresearch-env" { finresearch = [ ]; };
          package = pythonSet.finresearch;
        }).overrideAttrs
          (_: {
            meta = {
              description = "Deterministic investment-research data, modeling, and reporting workflows";
              homepage = "https://github.com/zydtiger/finresearch";
              license = lib.licenses.mit;
              platforms = lib.platforms.unix;
              mainProgram = "finresearch";
            };
          });
    in
    {
      packages = lib.genAttrs systems (
        system:
        let
          finresearch = mkPackage nixpkgs.legacyPackages.${system};
        in
        {
          inherit finresearch;
          default = finresearch;
        }
      );
    };
}
