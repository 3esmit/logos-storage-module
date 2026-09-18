{
  description = "Logos Storage Module";

  # Pull pre-built artifacts from the self-hosted Logos Attic cache(Nix binary cache).
  nixConfig = {
    extra-substituters = [ "https://cache.nix.logos.co/public" ];
    extra-trusted-public-keys = [ "public:l4HrXgL4nw246+LBh2SOJyhz64BoGegOYLheT/iIAPU=" ];
  };

  inputs = {
    logos-module-builder.url = "github:3esmit/logos-module-builder?rev=39b159144aa37f4000cfbbb1c348a8479ee0864e";
    logos-storage.url = "git+https://github.com/3esmit/logos-storage-nim?submodules=1&rev=21a00ace2e03811c9bf4dfc9ba4688ad6e1cbd00";
  };

  outputs = inputs@{ logos-module-builder, ... }:
    let
      module = logos-module-builder.lib.mkLogosModule {
        src = ./.;
        configFile = ./metadata.json;
        flakeInputs = inputs;
        externalLibInputs = {
          libstorage = {
            input = inputs.logos-storage;
            packages.default = "libstorage";
          };
        };
        tests = {
          dir = ./tests;
        };
      };

      nixpkgs = logos-module-builder.inputs.nixpkgs;
      lib = nixpkgs.lib;

      # Native systems plus the "x86_64-windows" cross target.
      systems = logos-module-builder.lib.common.systems;

      # No tests for x86_64-windows: its Windows binaries cannot run on the Linux builder.
      nativeSystems = builtins.filter (s: s != "x86_64-windows") systems;

      # Provide a custom tests package to build tests without in-build execution.
      testsPackage = system:
        (module.packages.${system}.unit-tests).overrideAttrs (old: {
          buildPhase = builtins.replaceStrings [ ''"$bin"'' ] [ ":" ] old.buildPhase;
        });

      testsApps = builtins.listToAttrs (map (system:
        let
          pkgs = import nixpkgs { inherit system; };
          unitTests = testsPackage system;
          runner = pkgs.writeShellScript "run-tests" ''
            set -u
            filter="''${1:-}"
            ran=0
            for bin in ${unitTests}/bin/*; do
              name="$(basename "$bin")"
              if [ -n "$filter" ] && ! printf '%s\n' "$name" | ${pkgs.gnugrep}/bin/grep -q -- "$filter"; then
                continue
              fi
              echo "=== $name ==="
              output="$(mktemp)"
              if ! "$bin" --json >"$output" 2>&1; then
                cat "$output"
                rm -f "$output"
                exit 1
              fi
              cat "$output"
              summary="$(tail -n 1 "$output")"
              rm -f "$output"
              if ! printf '%s\n' "$summary" | ${pkgs.jq}/bin/jq -e \
                  '(.failed | type) == "number" and .failed == 0' >/dev/null; then
                echo "Test binary reported failures or no JSON summary: $name" >&2
                exit 1
              fi
              ran=$((ran + 1))
            done
            if [ "$ran" -eq 0 ] && [ -n "$filter" ]; then
              echo "No test binary matched filter: $filter" >&2
              exit 1
            fi
          '';
        in {
          name = system;
          value = { tests = { type = "app"; program = toString runner; }; };
        }
      ) nativeSystems);

      existingApps = module.apps or {};
      mergedApps = builtins.listToAttrs (map (system: {
        name = system;
        value = (existingApps.${system} or {}) // (testsApps.${system} or {});
      }) systems);

      # Expose the test binaries as a buildable package so `nix build .#tests`
      # produces result/bin/{storage_module_tests,storage_module_integration_tests}.
      mergedPackages = builtins.listToAttrs (map (system: {
        name = system;
        value = (module.packages.${system} or {})
          // lib.optionalAttrs (builtins.elem system nativeSystems) {
               tests = testsPackage system;
             };
      }) systems);

    in module // { apps = mergedApps; packages = mergedPackages; };
}
