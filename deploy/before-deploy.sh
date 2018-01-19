#!/usr/bin/env bash

if [ "$TRAVIS_BRANCH" = 'master' ] && [ "$TRAVIS_PULL_REQUEST" == 'false' ]; then
    openssl aes-256-cbc -K $encrypted_97c7a3c439c7_key -iv $encrypted_97c7a3c439c7_iv \
      -in $DEPLOY_DIR/codesigning.asc.enc -out $DEPLOY_DIR/codesigning.asc -d
    gpg --fast-import $DEPLOY_DIR/codesigning.asc
fi
