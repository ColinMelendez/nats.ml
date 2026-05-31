{
  description = "NATS client SDK for OCaml";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";

  outputs =
    { nixpkgs, ... }:
    let
      systems = [
        "aarch64-darwin"
        "aarch64-linux"
        "x86_64-linux"
      ];
      forAllSystems = nixpkgs.lib.genAttrs systems;
    in
    {
      devShells = forAllSystems (
        system:
        let
          pkgs = import nixpkgs { inherit system; };
          base_packages = with pkgs; [
            curl
            gawk
            gmp
            git
            pkg-config
          ];
          ocaml_packages = with pkgs.ocamlPackages_latest; [
            ocaml
            dune_3
          ];
          interop_peer = pkgs.buildGoModule {
            pname = "nats-ocaml-interop-peer";
            version = "0.1.0";
            src = ./interop/nats-ocaml-interop-peer;
            vendorHash = "sha256-642/GXc90xVafKJ62minBhp6ZB7eULH9vvj1dQ2NqeM=";
            ldflags = [ "-s" "-w" ];
          };
          shell_hook = ''
            export LC_ALL=C
          '';
        in
        {
          integration = pkgs.mkShell {
            LC_ALL = "C";
            packages =
              base_packages
              ++ (with pkgs; [
                openssl
                shellcheck
                interop_peer
              ])
              ++ ocaml_packages;
            shellHook = shell_hook;
          };

          test = pkgs.mkShell {
            LC_ALL = "C";
            packages = base_packages ++ ocaml_packages;
            shellHook = shell_hook;
          };

          default = pkgs.mkShell {
            LC_ALL = "C";
            packages =
              base_packages
              ++ (with pkgs; [
                nixfmt
              ])
              ++ (with pkgs.ocamlPackages_latest; [
                ocaml
                dune_3
                odoc
                ocaml-lsp
                ocamlformat
              ]);
            shellHook = shell_hook;
          };
        }
      );
    };
}
