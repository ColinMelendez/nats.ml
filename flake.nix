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
