# docker container for lambda functions with conda
FROM continuumio/miniconda3

# Add Maintainer Info
LABEL maintainer="tori"

ARG env_name="wind-prod"
ENV env_name="$env_name"

ARG function_dir="/app"
ENV function_dir="$function_dir"

RUN mkdir -p "$function_dir"

# create app directory
WORKDIR "$function_dir"

# Create the environment
RUN mkdir -p env
COPY environment.yml env
RUN conda env create --prefix "env/$env_name" --file env/environment.yml

# Copy app
COPY src .

# run application
WORKDIR "$function_dir"
ENTRYPOINT "$function_dir/env/$env_name/bin/python" -m awslambdaric main.lambda_handler
