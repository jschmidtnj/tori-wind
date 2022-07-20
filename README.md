# Wind-Powered DAC

> analysis of best carbon capture plant locations

See [analysis.ipynb](./analysis/analysis.ipynb) for the results of this project.

## Getting Started

To get started locally, all you need to do is create the conda environment (see the [background section](#background)) and run whichever file you need in that environment. To run in the cloud, you need to have it configured correctly. Create an AWS account, then create two lambda functions with container instance types. You can host the container instances anywhere, but we used [AWS ECR](https://aws.amazon.com/ecr/) for the images. Build the image by following the instructions for pushing an image on ECR, in the [calculate](./calculate_lambda/) and [reduce](./reduce_lambda/) folders. After building, pushing, and creating the lambda functions, you need to upload the data files to s3. Create an s3 bucket and upload using the [AWS CLI](https://aws.amazon.com/cli/). Use a command similar to this one: `aws s3 sync ./World_2019/ s3://tori-calculate-wind-power/World_2019`. Once everything is uploaded and [IAM](https://aws.amazon.com/iam/) is configured so both lambda functions have access to the files, you can start the lambda functions with the [start_lambdas.py](./calculate_lambda/src/start_lambdas.py) files. The output of the files can be found in the same s3 folder. To generate the heat maps, look at the [analysis.ipynb](./analysis/analysis.ipynb) file.

## Background

There are two AWS Lambda functions, located in [calculate](./calculate_lambda/) and [reduce](./reduce_lambda/), which do the calculations. These functions can be run locally or in the cloud. To run locally, you need to have the `wind` conda environment created and activated, specified in the [environment.yml file](./calculate_lambda/environment.yml) (`conda env create -f ./environment.yml`). Next navigate to the [src directory](./calculate_lambda/src/) and execute the [start_lambdas.py file](./calculate_lambda/src/start_lambdas.py). Prompts are used for selecting different options, but they can also be specified through chanding the input values for the main function. You just need to specify run locally when prompted. There is another [`start_lambdas.py` file in the `reduce` directory](./reduce_lambda/src/start_lambdas.py) that works the same way.

If you are not familiar with cloud functions / lambda functions, you can read more about AWS Lambda [here](https://aws.amazon.com/lambda/). You can think of it like having a short-lived server in the cloud. They are utilized for two purposes in this project - to calculate the power output and SED, and combine all of the total outputs. It follows the [map reduce architecture](https://www.analyticsvidhya.com/blog/2014/05/introduction-mapreduce/), which is commonly used in big data projects, with software technologies like [Spark](https://spark.apache.org/) and [Hadoop](https://hadoop.apache.org/) specifically designed with this architecture in mind. For the sake of simplicity, it was decided to not use Spark, though Spark has some benefits with stream processing that could make this more efficient.

### Notes

- One feature of AWS Lambda that we are using is [Container Runtime](https://docs.aws.amazon.com/lambda/latest/dg/images-create.html), which is a way of running Lambda functions with [docker images](https://www.docker.com/). This allows us to package the Conda environment and dependencies in a base container, so it always behaves as expected and can be customized to have any type of dependency.
- There are two pickle files, [model.pkl](./calculate_lambda/src/model.pkl) and [poly.pkl](./calculate_lambda/src/poly.pkl). These were added so that the model did not need to train each time we ran the code. They are generated with the [generate_model.py file](./calculate_lambda/generate_model.py).
- All data interpreting / testing files can be found in the [analysis folder](./analysis/).
- This code was exclusively run in an Ubuntu [WSL 2](https://docs.microsoft.com/en-us/windows/wsl/about) runtime. This means that it behaves like Linux. It should work on Windows too, but that was not tested.
