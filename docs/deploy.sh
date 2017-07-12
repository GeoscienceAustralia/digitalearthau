#!/bin/bash

# Deploy script for http://geoscienceaustralia.github.io/digitalearthau/
# Builds sphinx docs and pushes to gh-pages branch
# Based on https://gist.github.com/domenic/ec8b0fc8ab45f39403dd

set -e # Exit with nonzero exit code if anything fails

cd "$(dirname "$0")" # cd into this directory

SOURCE_BRANCH="develop"
TARGET_BRANCH="gh-pages"
BUILD_PATH="_build/html"
COMMITTER_EMAIL=$(git show --format="%aE" -s)
AUTHOR_NAME_EMAIL=$(git show --format="%aN <%aE>" -s)
ENCRYPTION_LABEL="c4bf5207aec3"

function doCompile {
  pip install Sphinx sphinx_rtd_theme nbsphinx
  make html
}

# Pull requests and commits to other branches shouldn't try to deploy, just build to verify
if [ "$TRAVIS_PULL_REQUEST" != "false" ] || [ "$TRAVIS_BRANCH" != "$SOURCE_BRANCH" ]; then
    echo "Skipping deploy; just doing a build."
    doCompile
    exit 0
fi

# Save some useful information
REPO=$(git config remote.origin.url)
SSH_REPO=${REPO/https:\/\/github.com\//git@github.com:}
SHA=$(git rev-parse --verify HEAD)

# Clone the existing gh-pages for this repo into out/
# Create a new empty branch if gh-pages doesn't exist yet (should only happen on first deply)
git clone "$REPO" "$BUILD_PATH"
pushd $BUILD_PATH
git checkout $TARGET_BRANCH || git checkout --orphan $TARGET_BRANCH
# Clean out existing contents
rm -rf ./*
popd

# Run our compile script
doCompile

# Now let's go have some fun with the cloned repo
pushd $BUILD_PATH
git config user.name "Travis CI"
git config user.email "$COMMITTER_EMAIL"

# If there are no changes to the compiled out (e.g. this is a README update) then just bail.
if git diff --quiet; then
    echo "No changes to the output on this push; exiting."
    popd
    exit 0
fi

# Commit the "changes", i.e. the new version.
# The delta will show diffs between new and old versions.
git add -A .
git commit --author="${AUTHOR_NAME_EMAIL}" -m "Deploy to GitHub Pages: ${SHA}"
popd

# Get the deploy key by using Travis's stored variables to decrypt deploy_key.enc
ENCRYPTED_KEY_VAR="encrypted_${ENCRYPTION_LABEL}_key"
ENCRYPTED_IV_VAR="encrypted_${ENCRYPTION_LABEL}_iv"
ENCRYPTED_KEY=${!ENCRYPTED_KEY_VAR}
ENCRYPTED_IV=${!ENCRYPTED_IV_VAR}
openssl aes-256-cbc -K $ENCRYPTED_KEY -iv $ENCRYPTED_IV -in dea-docs-gen.enc -out deploy_key -d
chmod 600 deploy_key
eval "$(ssh-agent -s)"
ssh-add deploy_key

# Now that we're all set up, we can push.
pushd $BUILD_PATH
git push "$SSH_REPO" "$TARGET_BRANCH"
popd

echo "deploy.sh done"
exit 0
