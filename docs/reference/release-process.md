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

# Commit
git add .
git commit -m "Releasing version $(lightbus version --pyproject)"

# Make docs
mike deploy v$(lightbus version --pyproject --docs) --message="Build docs for release of $(lightbus version --pyproject)"

# Tagging and branching
git tag "v$(lightbus version --pyproject)"
git branch "v$(lightbus version --pyproject)"
git push origin \
    refs/tags/"v$(lightbus version --pyproject)" \
    refs/heads/"v$(lightbus version --pyproject)" \
    master \
    gh-pages

# Wait for CI to pass: https://circleci.com/gh/adamcharnock/lightbus

# Build and publish
poetry publish --build
```
