# Lightbus release process

Lightbus releases are performed as follows:

```shell
# Update the setup.py
dephell convert
black setup.py

# Ensure poetry.lock is up to date
poetry lock

# Version bump
poetry version {patch,minor,major,prepatch,preminor,premajor,prerelease}

export VERSION=$(lightbus version --pyproject)   # v1.2.3
export VERSION_DOCS=$(lightbus version --pyproject --docs)  # v1.2

# Commit
git add .
git commit -m "Releasing version $VERSION"

# Make docs
git checkout gh-pages
git pull origin gh-pages
git checkout master

mike deploy v$VERSION_DOCS --message="Build docs for release of $VERSION"
mike delete latest
mike alias v$VERSION_DOCS latest

# Tagging and branching
git tag "v$VERSION"
git branch "v$VERSION"
git push origin \
    refs/tags/"v$VERSION" \
    refs/heads/"v$VERSION" \
    master \
    gh-pages

# Wait for CI to pass: https://circleci.com/gh/adamcharnock/lightbus

# Build and publish
poetry publish --build
```
