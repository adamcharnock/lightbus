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

# Tagging and branching
git tag "v$(lightbus version --pyproject)"
git branch "v$(lightbus version --pyproject)"
git push origin "v$(lightbus version --pyproject)"
git push origin "v$(lightbus version --pyproject)"

# Build and publish
poetry publish --build
```
