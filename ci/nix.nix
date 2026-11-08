# The Nix workflow. .github/workflows/nix.yml is emitted from this and is never
# edited by hand:
#
#   nix eval --json --file ci/nix.nix | jq . > .github/workflows/nix.yml
#
# JSON is YAML, so GitHub reads the emitted file as it is.
let
  # Every job starts from the same three steps.
  setup = [
    {
      name = "Checkout repository";
      uses = "actions/checkout@v5";
    }

    {
      name = "Install Nix";
      uses = "cachix/install-nix-action@v30";
      "with".extra_nix_config = ''
        experimental-features = nix-command flakes
      '';
    }

    {
      name = "Setup Cachix";
      uses = "cachix/cachix-action@v14";
      "with" = {
        name = "qntx";
        authToken = "\${{ secrets.CACHIX_AUTH_TOKEN }}";
      };
    }
  ];

  # What a build job does after setup, the same on every runner.
  build = [
    {
      name = "Build pyre";
      run = "nix build --show-trace";
    }

    # --include-ignored is what runs DIRE. Those tests reach PyPI, so they
    # cannot run inside `nix build`'s sandbox, and this step has network.
    {
      name = "Run tests";
      run = "nix develop -c cargo test -- --include-ignored";
    }

    # DIRE is pyre wrapped with its own Python environment, the shape every
    # consumer builds. Building it here is what keeps the example true.
    {
      name = "Build dire";
      run = "nix build --show-trace .#dire";
    }

    {
      name = "Push to Cachix";
      run = "nix build --print-out-paths --no-link | cachix push qntx";
    }
  ];

  clippy = system: [
    {
      name = "Run clippy";
      run = "nix build --show-trace .#checks.${system}.clippy";
    }
  ];

  # Linux is what has to work, so Linux gates every push and pull request.
  # macOS is the slowest job of the run: it builds on the weekly schedule or
  # when asked by hand, and on no push and no pull request.
  weeklyOrAsked = "github.event_name == 'schedule' || github.event_name == 'workflow_dispatch'";
in
{
  name = "Nix";

  on = {
    push.branches = [ "main" ];
    pull_request = { };
    # Monday 06:00 UTC, the week's first day.
    schedule = [{ cron = "0 6 * * 1"; }];
    workflow_dispatch = { };
  };

  jobs = {
    # The matrix keeps the check name `build (ubuntu-latest)`.
    build = {
      strategy.matrix.os = [ "ubuntu-latest" ];
      runs-on = "\${{ matrix.os }}";
      steps = setup ++ build;
    };

    clippy = {
      strategy.matrix.os = [ "ubuntu-latest" ];
      runs-on = "\${{ matrix.os }}";
      steps = setup ++ clippy "x86_64-linux";
    };

    build-macos = {
      "if" = weeklyOrAsked;
      strategy.matrix.os = [ "macos-latest" ];
      runs-on = "\${{ matrix.os }}";
      steps = setup ++ build;
    };

    clippy-macos = {
      "if" = weeklyOrAsked;
      strategy.matrix.os = [ "macos-latest" ];
      runs-on = "\${{ matrix.os }}";
      steps = setup ++ clippy "aarch64-darwin";
    };
  };
}
