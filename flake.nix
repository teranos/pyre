{
  description = "Pyre - Python runtime engine for QNTX";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    qntx-src = {
      url = "github:teranos/QNTX";
      flake = false;
    };
  };

  outputs = { self, nixpkgs, flake-utils, qntx-src }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs { inherit system; };

        outputHashes = {
          "qntx-core-0.2.44" = "sha256-FK6FsDgkF+qOkRxk5xZP/IagRGpYKS2pmYr9WE1aZCc=";
        };

        pyre = pkgs.rustPlatform.buildRustPackage {
          pname = "pyre";
          version = self.rev or "dev";
          src = ./.;

          cargoLock = {
            lockFile = ./Cargo.lock;
            inherit outputHashes;
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

          # Proto files live in QNTX repo — tell qntx-grpc build.rs where to find them
          QNTX_PROTO_DIR = "${qntx-src}/plugin/grpc/protocol";

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

        # DIRE — Development In Runtime Environments. pyre wrapped with its own
        # Python environment: the shape capy and icpy each rebuilt from
        # nothing. An output here, not a second flake, so it shares this lock.
        direPython = pkgs.python313.withPackages (ps: with ps; [ requests ]);

        direVersion = "0.1.0";

        dire = pkgs.rustPlatform.buildRustPackage {
          pname = "qntx-dire-plugin";
          version = direVersion;
          src = ./.;

          QNTX_PLUGIN_VERSION = direVersion;

          cargoLock = {
            lockFile = ./Cargo.lock;
            inherit outputHashes;
          };

          buildInputs = with pkgs; [ protobuf direPython openssl ];
          nativeBuildInputs = with pkgs; [ pkg-config protobuf makeWrapper ];

          QNTX_PROTO_DIR = "${qntx-src}/plugin/grpc/protocol";
          PYO3_PYTHON = "${direPython}/bin/python3";

          # The DIRE tests reach PyPI and the sandbox has no network. They run
          # under `cargo test -- --include-ignored` in CI, which does.
          doCheck = false;

          postInstall = ''
            mv $out/bin/pyre $out/bin/qntx-dire-plugin
          '' + pkgs.lib.optionalString pkgs.stdenv.isLinux ''
            patchelf --set-rpath "${pkgs.lib.makeLibraryPath [ direPython ]}:$(patchelf --print-rpath $out/bin/qntx-dire-plugin)" \
              $out/bin/qntx-dire-plugin
          '' + pkgs.lib.optionalString pkgs.stdenv.isDarwin ''
            install_name_tool -add_rpath "${pkgs.lib.makeLibraryPath [ direPython ]}" \
              $out/bin/qntx-dire-plugin
          '' + ''
            wrapProgram $out/bin/qntx-dire-plugin \
              --prefix PYTHONPATH : "${direPython}/${direPython.sitePackages}" \
              --add-flags "--name dire"
          '';
        };

        # Clippy check
        pyre-clippy = pkgs.rustPlatform.buildRustPackage {
          pname = "pyre-clippy";
          version = self.rev or "dev";
          src = ./.;

          cargoLock = {
            lockFile = ./Cargo.lock;
            inherit outputHashes;
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

          QNTX_PROTO_DIR = "${qntx-src}/plugin/grpc/protocol";
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
          dire = dire;
          dire-python = direPython;
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

          QNTX_PROTO_DIR = "${qntx-src}/plugin/grpc/protocol";
          PYO3_PYTHON = "${pkgs.python313}/bin/python3";
        };
      });
}
