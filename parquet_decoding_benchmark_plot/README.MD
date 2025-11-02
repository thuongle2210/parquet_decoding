# parquet_decoding_benchmark_plot Setup and Usage

## Step 1: Set up environment
```
cd parquet_decoding_benchmark_plot
pyenv install $(python --version 2>&1 | awk '{print $2}')
```
## Step 2: Create and activate virtual environment
```
pyenv virtualenv $(python --version 2>&1 | awk '{print $2}') parquet_decoding_benchmark_plot_venv
pyenv activate parquet_decoding_benchmark_plot_venv
pyenv local parquet_decoding_benchmark_plot_venv
```
## Step 3: Install dependencies
```
pip install -r requirements.txt
```
## Step 4: Run plotting script

```python plot.py```

## Step 5: Deactivate virtual environment

```pyenv deactivate```