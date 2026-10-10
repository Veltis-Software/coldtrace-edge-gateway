# Contributing

Use short-lived `feature/<area>-<change>` or `fix/<area>-<change>` branches and
review changes through pull requests to `main`. Commits use Conventional
Commits; component versions follow SemVer. `0.1.0-SNAPSHOT` is development work,
not a stable release. Tag released source; never reuse a published version tag.

Keep code and API names in English. Java uses Google Java Format; Python uses
Black with four-space indentation. Run the component tests and image/config
checks described in README before requesting review. Include the concrete
behavior changed and validation results in the PR. Keep schema changes under
Flyway and integration event changes compatible with versioned contracts.
