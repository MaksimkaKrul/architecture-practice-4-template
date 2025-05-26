# entry.sh
#!/bin/sh

echo "--- Debugging entry.sh ---"
echo "Current directory: $(pwd)"
echo "Arguments received: $@"
echo "Binary name argument: $1"
echo "Listing contents of current directory (/opt/practice-4/):"
ls -la /opt/practice-4/
echo "--------------------------"

bin=$1
shift

if [ -z "$bin" ]; then
  echo "Error: binary name not specified"
  exit 1
fi

if [ ! -f "$bin" ]; then
  echo "Error: binary '$bin' not found at /opt/practice-4/$bin. Exiting."
  exit 1
fi

echo "Attempting to execute: ./$bin $@"
exec "./$bin" "$@" 