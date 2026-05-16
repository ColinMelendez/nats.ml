{
  description = "NATS client SDK for OCaml";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";

  outputs = { nixpkgs, ... }:
    let
      systems = [
        "aarch64-darwin"
        "aarch64-linux"
        "x86_64-linux"
      ];
      forAllSystems = nixpkgs.lib.genAttrs systems;
    in {
      devShells = forAllSystems (system:
        let
          pkgs = import nixpkgs { inherit system; };
        in {
          test = pkgs.mkShell {
            packages = with pkgs; [
              curl
              gmp
              git
              pkg-config
            ] ++ (with ocamlPackages_latest; [
              ocaml
              dune_3
            ]);
          };

          default = pkgs.mkShell {
            packages = with pkgs; [
              curl
              gmp
              git
              pkg-config
              nixfmt
            ] ++ (with ocamlPackages_latest; [
              ocaml
              dune_3
              odoc
              ocaml-lsp
              ocamlformat
            ]);
          };
        });
    };
}
