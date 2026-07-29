set -e
./py_to_exec.sh

cd RPM
./buildrpm $BUILDRPM_FLAGS
