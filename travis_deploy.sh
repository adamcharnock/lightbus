#!/usr/bin/env bash

set -e

openssl aes-256-cbc -K $encrypted_739cc9c14904_key -iv $encrypted_739cc9c14904_iv -in docs/deploy_key.enc -out deploy_key -d

set -x

eval "$(ssh-agent -s)"
chmod 600 deploy_key
ssh-add deploy_key

rm docs/deploy_key.enc
rm deploy_key

git remote remove origin
git remote add origin git@github.com:adamcharnock/lightbus.git

mkdocs gh-deploy --force

