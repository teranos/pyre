{
  description = "dire — pyre wrapped with its own Python environment";

  # Development In Runtime Environments. The pattern every pyre consumer ends
  # up rebuilding: wrap the plugin with `withPackages` so its modules are
  # pinned and owned, then give it a name. capy does it for an ad account.

  # It ships here so the pattern comes with pyre rather than being
  # rediscovered, and so CI builds it. The interpreter below has no pip, which
  # is the configuration that made pyre's own package management useless.

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    pyre-src = {
      url = "path:..";
      flake = false;
    };
    qntx-src = {
      url = "github:teranos/QNTX";
      flake = false;
    };
  };

  outputs = { self, nixpkgs, flake-utils, pyre-src, qntx-src }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs { inherit system; };

        # Baked in, pinned, owned. Anything else a handler wants it asks for at
        # runtime through /uv/install, which is the whole point of DIRE.
        pythonWithPackages = pkgs.python313.withPackages (ps: with ps; [
          requests
        ]);

        outputHashes = {
          "qntx-core-0.2.44" = "sha256-FK6FsDgkF+qOkRxk5xZP/IagRGpYKS2pmYr9WE1aZCc=";
        };

        direVersion = "0.1.0";

        dire = pkgs.rustPlatform.buildRustPackage {
          pname = "qntx-dire-plugin";
          version = direVersion;
          src = pyre-src;

          QNTX_PLUGIN_VERSION = direVersion;

          cargoLock = {
            lockFile = "${pyre-src}/Cargo.lock";
            inherit outputHashes;
          };

          buildInputs = with pkgs; [ protobuf pythonWithPackages openssl ];
          nativeBuildInputs = with pkgs; [ pkg-config protobuf makeWrapper ];

          QNTX_PROTO_DIR = "${qntx-src}/plugin/grpc/protocol";
          PYO3_PYTHON = "${pythonWithPackages}/bin/python3";

          # The DIRE tests reach PyPI, and the sandbox has no network. They run
          # under `cargo test` in pyre's CI, which is where they belong.
          doCheck = false;

          postInstall = ''
            mv $out/bin/pyre $out/bin/qntx-dire-plugin
          '' + pkgs.lib.optionalString pkgs.stdenv.isLinux ''
            patchelf --set-rpath "${pkgs.lib.makeLibraryPath [ pythonWithPackages ]}:$(patchelf --print-rpath $out/bin/qntx-dire-plugin)" \
              $out/bin/qntx-dire-plugin
          '' + pkgs.lib.optionalString pkgs.stdenv.isDarwin ''
            install_name_tool -add_rpath "${pkgs.lib.makeLibraryPath [ pythonWithPackages ]}" \
              $out/bin/qntx-dire-plugin
          '' + ''
            wrapProgram $out/bin/qntx-dire-plugin \
              --prefix PYTHONPATH : "${pythonWithPackages}/${pythonWithPackages.sitePackages}" \
              --add-flags "--name dire"
          '';
        };
      in
      {
        packages = {
          default = dire;
          dire = dire;
          python = pythonWithPackages;
        };

        apps.default = {
          type = "app";
          program = "${dire}/bin/qntx-dire-plugin";
        };
      });
}
