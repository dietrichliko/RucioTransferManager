# Rucio Transfer Managing

A bookkeeping tool to track a large number of [RUCIO](https://rucio.cern.ch/) requests.
Datasets, their files and the rucio requests are stored in  a SQL databased.

## States

A dataset in RucioTransferManager can have following states

* __Defined:__ The datasets and its file content is defined and stored in the database
* __Subscribed:__ The dataset is subscribed in rucio and transfers are done by the system
* __Transferred:__ All files have been transferred to the target site
* __Done:__ All files have been checked with ```xrdalder32```.

# Install
---------

```bash
git clone https://github.com/dietrichliko/RucioTransferManager.git
cd RucioTransferManager

mamba create -n mrt --file packages.txt
conda activate mrt
find $CONDA_PREFIX/lib/python3.11/site-packages -name direct_url.json -delete
poetry install
```

# Commands

```text
$ rtm --help
Usage: rtm [OPTIONS] COMMAND [ARGS]...

  Rucio Transfer Manager - command line interface.

Options:
  -l, --log-level [NOTSET|DEBUG|INFO|WARNING|ERROR|CRITICAL]
                                  Set logging level  [default: INFO]
  -c, --config FILE               Configuration File  [default: /users/dietric
                                  h.liko/.config/RucioTransferManager/rtm.toml
                                  ]
  --help                          Show this message and exit.

Commands:
  define     Define dataset in the DB
  delete     Delete subscription.
  status     List status of transfers.
  subscribe  Start transfer.
  verify     verify the transferred dataset.
  whoami     Effective rucio client account.
```