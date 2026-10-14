#!/bin/sh
# list-append under SERIALIZABLE while the server is killed with SIGKILL and restarted, then Elle
cd "$(dirname "$0")/.."
dir=${DIR:-/tmp/isodb-ellecrash}; port=15438; out=${1:-/tmp/isodb-ellecrash.edn}
rm -rf $dir
./isodb -listen 127.0.0.1:$port -data $dir > /dev/null 2>&1 & pid=$!
sleep 0.5
${PYTHON:-python3} jepsen/listappend.py --port $port --txns ${TXNS:-6000} --out $out & load=$!
kills=0
while kill -0 $load 2>/dev/null; do
    sleep "0.$(shuf -i 5-15 -n 1)"
    kill -9 $pid 2>/dev/null; wait $pid 2>/dev/null; kills=$((kills + 1))
    ./isodb -listen 127.0.0.1:$port -data $dir > /dev/null 2>&1 & pid=$!
done
wait $load
kill $pid
echo "kills: $kills"
java -jar ${ELLE:-elle-cli.jar} -m list-append -c strict-serializable -f edn $out
