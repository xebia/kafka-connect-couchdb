#!/usr/bin/env bash

if [ "$TRAVIS_BRANCH" = 'master' ] && [ "$TRAVIS_PULL_REQUEST" == 'false' ]; then
  if [ ! -z "$TRAVIS_TAG" ]; then
    echo "on a tag -> set pom.xml <version> to $TRAVIS_TAG"
    mvn --settings $DEPLOY_DIR/settings.xml org.codehaus.mojo:versions-maven-plugin:2.5:set -DnewVersion=$TRAVIS_TAG 1>/dev/null 2>/dev/null
  else
    echo "not on a tag -> keep snapshot version in pom.xml"
  fi

  mvn deploy --settings $DEPLOY_DIR/settings.xml -DperformRelease=true -DskipTests=true
fi
