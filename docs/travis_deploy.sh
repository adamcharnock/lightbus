#!/usr/bin/env bash

set -e

openssl aes-256-cbc -K $encrypted_739cc9c14904_key -iv $encrypted_739cc9c14904_iv -in docs/deploy_key.enc -out docs/deploy_key -d

set -x

eval "$(ssh-agent -s)"
chmod 600 docs/deploy_key
ssh-add docs/deploy_key

git remote remove origin
git remote add origin git@github.com:adamcharnock/lightbus.git

mkdocs gh-deploy --force