# Contributing

## GitFlow

`main` contains reviewed release checkpoints. `develop` integrates work for the
next increment. Start each feature from `develop` using
`feature/<story-id>-<description>` and open its PR against `develop`.

Use `release/<major.minor.patch>` to prepare a release from `develop`; validate
it, merge it into `main` and back into `develop`, then tag `v<major.minor.patch>`.
Use `hotfix/<major.minor.patch>` from `main` for an urgent release correction,
and merge the fix back into both long-lived branches. Temporary branches are
removed after merging; do not create empty release/hotfix branches in advance.

## Commits, versions and validation

Use Conventional Commits and SemVer. `0.1.0-SNAPSHOT` denotes development;
stable release versions require a version bump and corresponding release tag.
Do not move or reuse a published version tag. The initial shared source tag
`v0.1.0-sprint1` is an immutable dependency checkpoint, not a stable Maven release.

Keep code and API names in English. Java uses Google Java Format; Python uses
Black with four-space indentation. Run the component tests and image/config
checks in README before review. Include the concrete behavior changed and
validation in each PR. Keep schema changes in Flyway and integration event
changes compatible with versioned contracts.
