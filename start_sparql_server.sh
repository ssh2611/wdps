KB_PORT=9090
KB_BIN=/home/jurbani/trident/build/trident
KB_PATH=/home/jurbani/data/motherkb-trident

echo "Lauching an instance of the Trident server on a random node in the cluster ..."
prun -o .kb_log -v -np 1 $KB_BIN server -i $KB_PATH --port $KB_PORT </dev/null 2> .kb_node &
echo "Waiting 5 seconds for trident to set up (use 'preserve -llist' to see if the node has been allocated)"
until [ -n "$KB_NODE" ]; do KB_NODE=$(cat .kb_node | grep '^:' | grep -oP '(node...)'); done
sleep 5
KB_PID=$!
echo "Trident should be running now on node $KB_NODE:$KB_PORT (connected to process $KB_PID)"
