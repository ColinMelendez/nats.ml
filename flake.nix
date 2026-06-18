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
          pkgs = import nixpkgs {
            inherit system;
            overlays = [
              (_: prev: {
                dune_3 = prev.dune_3.overrideAttrs (_: rec {
                  version = "3.24.2";
                  src = prev.fetchurl {
                    url = "https://github.com/ocaml/dune/releases/download/${version}/dune-${version}.tbz";
                    hash = "sha256-RyeYaRsCFtr1OHCfD0cDs2F+8krQhmyQlgaLqrpNdio=";
                  };
                });
              })
            ];
          };
          base_packages = with pkgs; [
            curl
            gawk
            gmp
            git
            pkg-config
          ];
          ocamlPackages = pkgs.ocaml-ng.ocamlPackages_5_5.overrideScope (
            final: prev: {
              cppo = prev.cppo.overrideAttrs (old: {
                postPatch = (old.postPatch or "") + ''
                  # Dune 3.24 preserves the leading ./ in dependency paths.
                  # Keep cppo's diagnostic fixtures aligned without disabling
                  # its test suite.
                  for reference in test/*.ref; do
                    input="$(basename "''${reference%.ref}").cppo"
                    if grep -Fq "\"$input\"" "$reference"; then
                      substituteInPlace "$reference" \
                        --replace-fail "\"$input\"" "\"./$input\""
                    fi
                    if grep -Fq "CPPO_FILE=$input" "$reference"; then
                      substituteInPlace "$reference" \
                        --replace-fail "CPPO_FILE=$input" "CPPO_FILE=./$input"
                    fi
                  done
                '';
              });
              ocaml =
                (prev.ocaml.override {
                  flambdaSupport = true;
                }).overrideAttrs
                  (old: {
                    configureFlags = (old.configureFlags or [ ]) ++ [ "--disable-flat-float-array" ];
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
            vendorHash = "sha256-8t5NY4M6KtSLVgKhRbRsKqzx1PncBVd31JtD6Pl4Sqk=";
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
                go
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
