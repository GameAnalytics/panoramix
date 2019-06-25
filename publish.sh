#!/usr/bin/env bash
sed -i "s|version: .*|version: \"$(git describe --tags)\",|g" mix.exs && mix hex.publish --yes