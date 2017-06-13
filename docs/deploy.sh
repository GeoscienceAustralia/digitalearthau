#!/bin/bash
set -e # Exit with nonzero exit code if anything fails

cd "$(dirname "$0")" # cd into this directory

SOURCE_BRANCH="wip-docs"  # Should be "master" when in production
TARGET_BRANCH="gh-pages"

function doCompile {
  make html
}

# Pull requests and commits to other branches shouldn't try to deploy, just build to verify
if [ "$TRAVIS_PULL_REQUEST" != "false" -o "$TRAVIS_BRANCH" != "$SOURCE_BRANCH" ]; then
    echo "Skipping deploy; just doing a build."
    doCompile
    exit 0
fi

# Save some useful information
REPO=`git config remote.origin.url`
SSH_REPO=${REPO/https:\/\/github.com\//git@github.com:}
SHA=`git rev-parse --verify HEAD`

# Clone the existing gh-pages for this repo into out/
# Create a new empty branch if gh-pages doesn't exist yet (should only happen on first deply)
git clone $REPO _build/html
cd _build/html
git checkout $TARGET_BRANCH || git checkout --orphan $TARGET_BRANCH
cd ../..

# Clean out existing contents
rm -rf _build/html/**/* || exit 0

# Run our compile script
doCompile

# Now let's go have some fun with the cloned repo
cd out
git config user.name "Travis CI"
git config user.email "$COMMIT_AUTHOR_EMAIL"

# If there are no changes to the compiled out (e.g. this is a README update) then just bail.
if git diff --quiet; then
    echo "No changes to the output on this push; exiting."
    exit 0
fi

echo "deploy.sh done"
exit 0
