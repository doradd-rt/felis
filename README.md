# Felis

A fork of Caracal (SOSP'21) source code.
This fork involves the main following changes.
- [TODO] add description
- [TODO] add new parameters explanation


# Papers

*Caracal: Contention Management with Deterministic Concurrency Control* - SOSP'21 [Paper](https://dl.acm.org/doi/10.1145/3477132.3483591) [Slides](https://docs.google.com/presentation/d/1yTEkQ7fRucArBguChkD3p_b6TOoqPdAK_rfSb7DBwog/edit?usp=sharing) [Talk](https://youtu.be/QZ8sMvck654) [Long Talk](https://youtu.be/NUWl4dSfA1c)

Build
=====

1. First, install dependencies

```
sudo apt-get update
sudo apt-get install -y default-jdk ant python2 git python-is-python3 wget gnupg lsb-release software-properties-common
wget https://apt.llvm.org/llvm.sh
chmod +x llvm.sh
sudo ./llvm.sh 14
sudo apt update
sudo apt install -y clang-14 libc++-14-dev libc++abi-14-dev lld-14
```
and install buck from source.
```
git clone https://github.com/facebook/buck.git
cd buck
ant
./bin/buck build --show-output buck
sudo ln -sf "$(pwd)/bin/buck" /usr/local/bin/buck
```

2. Now you can use `buck` to build. 

```
buck build db
```

will generate the debug binary to `buck-out/gen/db#debug`. If you need
optimized build, you can run

```
buck build db_release
```

to generate the release binary to `buck-out/gen/db#release`.


Run
===

Setting Things Up
-----------------

Felis need to use HugePages for memory allocation (to reduce
the TLB misses). Common CSL cluster machines should have these already
setup, and you may skip this step. The following pre-allocates 400GB
of HugePages. You can adjust the amount depending on your memory
size. (Each HugePage is 2MB by default in Linux.)

```
echo 51200 > /proc/sys/vm/nr_hugepages
```

Run the Controller
----------------

To run the workload, the [felis-controller](https://github.com/doradd-rt/felis-controller) 
in another repo is needed. Please follow the README to setup the controller.

First, you need to enter the config for the nodes and controller, in
`config.json` in felis-controller.

Then, run the controller. We usually run the controller on localhost
when doing single-node experiments, and on a separate machine when
doing distributed experiments. It doesn't really matter though.

As long as the configuration doesn't change, you can let the controller
run all the time.

Start the database on each node
-------------------------------

Once the controller is initialized via 
```
java -jar out/FelisController/assembly.dest/out.jar config.json`)
```
on each node you can run:

```
buck-out/gen/db#release -c 127.0.0.1:3148 -n host1 -w ycsb -Xcpu24 -Xmem18G \
-XVHandleBatchAppend -XEpochSize10000 -XNrEpoch100 -XInterArrivalexp:20000 \
-XLogFile/home/<user>/ppopp-artifact/doradd/scripts/ycsb/ycsb_uniform_no_cont.txt
```

`-c` is the felis-controller IP address (<rpc_port> and <http_port>
below are specified in config.json as well), `-n` is the host name for
this node, and `-w` means the workload it will run (tpcc/ycsb).

`-X` are for the extended arguments. For a list of `-X`, please refer
to `opts.h`. Mostly you will need `-Xcpu` and `-Xmem` to specify how
many cores and how much memory to use. (Currently, number of CPU must
be multiple of 8. That's a bug, but we don't have time to fix it
though.)

Start running the workload
--------------------------

The node will initialize workload dataset and once they are idle, they
are waiting for further commands from the controller. When all of them
finish initialization, you can tell the controller that everybody can
proceed:

```
curl localhost:8666/broadcast/ -d '{"type": "status_change", "status": "connecting"}'
```

Logs
----

If you are running the debug version, the logging level is "debug" by
default, otherwise, the logging level is "info". You can always tune
the debugging level by setting the `LOGGER` environmental
variable. Possible values for `LOGGER` are: `trace`, `debug`, `info`,
`warning`, `error`, `critical`, `off`.

The debug level will output to a log file named `dbg-hostname.log`
where hostname is your node name. This is to prevent debugging log
flooding your screen.


to build the test binary. Then run the `buck-out/gen/dbtest` to run
all unit tests. We use google-test. To run partial test, please look
at
https://github.com/google/googletest/blob/master/googletest/docs/advanced.md#running-a-subset-of-the-tests
.
