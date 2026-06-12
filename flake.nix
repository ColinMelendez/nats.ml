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
          ocamlPackages = pkgs.ocamlPackages_latest.overrideScope (
            final: prev: {
              ocaml = prev.ocaml.overrideAttrs (old: {
                nativeBuildInputs = (old.nativeBuildInputs or [ ]) ++ [ pkgs.pkg-config ];
                buildInputs = (old.buildInputs or [ ]) ++ [ pkgs.zstd ];
                # The zstd-enabled compiler is rebuilt locally; the repository
                # checks remain enabled, so do not repeat OCaml's long upstream
                # test suite in every developer toolchain build.
                doCheck = false;
              });
            }
          );
          ocaml_packages = with ocamlPackages; [
            ocaml
            dune_3
          ];
          interop_peer = pkgs.buildGoModule {
            pname = "nats-ocaml-interop-peer";
            version = "0.1.0";
            src = ./interop/nats-ocaml-interop-peer;
            vendorHash = "sha256-iAnaEm8vuPf/Px4e3tOk0uRvBjPlslN9rOwNb1+OzWs=";
            ldflags = [
              "-s"
              "-w"
            ];
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
                nsc
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
              ++ (with ocamlPackages; [
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
