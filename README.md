

Install
-------

```bash
mamba create -n mrt --file packages.txt
conda activate mrt
find $CONDA_PREFIX/lib/python3.11/site-packages -name direct_url.json -delete
poetry install
```