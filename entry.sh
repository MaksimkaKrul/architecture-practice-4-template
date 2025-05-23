#!/bin/sh

bin=$1
shift

if [ -z "$bin" ]; then
  echo "Error: binary name not specified"
  exit 1
fi

if [ ! -f "$bin" ]; then  
  echo "Error: binary '$bin' not found"
  ls -la /opt/practice-4/
  exit 1
fi

exec "./$bin" "$@"