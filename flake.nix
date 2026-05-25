{
  description = "Pyre - Python runtime engine for QNTX";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs { inherit system; };

        pyre = pkgs.rustPlatform.buildRustPackage {
          pname = "pyre";
          version = self.rev or "dev";
          src = ./.;

          cargoLock = {
            lockFile = ./Cargo.lock;
          };

          buildInputs = with pkgs; [
            protobuf
            python313
            openssl
          ];

          nativeBuildInputs = with pkgs; [
            pkg-config
            protobuf
          ];

          # Set Python for PyO3
          PYO3_PYTHON = "${pkgs.python313}/bin/python3";

          # Set rpath/install_name to find Python at runtime
          postFixup = pkgs.lib.optionalString pkgs.stdenv.isLinux ''
            patchelf --set-rpath "${pkgs.lib.makeLibraryPath [ pkgs.python313 ]}:$(patchelf --print-rpath $out/bin/pyre)" \
              $out/bin/pyre
          '' + pkgs.lib.optionalString pkgs.stdenv.isDarwin ''
            install_name_tool -add_rpath "${pkgs.lib.makeLibraryPath [ pkgs.python313 ]}" \
              $out/bin/pyre
          '';
        };

        # Clippy check
        pyre-clippy = pkgs.rustPlatform.buildRustPackage {
          pname = "pyre-clippy";
          version = self.rev or "dev";
          src = ./.;

          cargoLock = {
            lockFile = ./Cargo.lock;
          };

          nativeBuildInputs = with pkgs; [
            pkg-config
            protobuf
            clippy
          ];

          buildInputs = with pkgs; [
            protobuf
            python313
            openssl
          ];

          PYO3_PYTHON = "${pkgs.python313}/bin/python3";

          buildPhase = ''
            cargo clippy --all-targets -- -D warnings
          '';

          installPhase = ''
            mkdir -p $out
            echo "Clippy passed" > $out/result
          '';

          doCheck = false;
        };
      in
      {
        packages = {
          default = pyre;
          pyre = pyre;
        };

        checks = {
          clippy = pyre-clippy;
        };

        apps.default = {
          type = "app";
          program = "${pyre}/bin/pyre";
        };

        devShells.default = pkgs.mkShell {
          buildInputs = with pkgs; [
            cargo
            rustc
            rust-analyzer
            clippy
            pkg-config
            protobuf
            python313
            openssl
          ];

          PYO3_PYTHON = "${pkgs.python313}/bin/python3";
        };
      });
}
