# Lightbus release process

Lightbus releases are performed as follows:

```shell
# Version bump
poetry version {patch,minor,major,prepatch,preminor,premajor,prerelease}

# Tagging and branching
git tag "v$(lightbus version --pyproject)"
git branch "v$(lightbus version --pyproject)"
git push origin "v$(lightbus version --pyproject)"
git push origin "v$(lightbus version --pyproject)"

# Build and publish
poetry publish --build
```
