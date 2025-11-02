# step 1:
setup env:
pyenv install $(python --version 2>&1 | awk '{print $2}')

# step 2:
create env:


pyenv virtualenv $(python --version 2>&1 | awk '{print $2}') parquet_decoding_benchmark_plot_venv

pyenv activate parquet_decoding_benchmark_plot_venv

pyenv local parquet_decoding_benchmark_plot_venv

# step 3:
install dependencies:

# step 4: set local 
pyenv local venv_myproject

# step 5: plotting...
pyenv local venv_myproject

# step 6: deactive pyenv deactivate



