# download

> download data from mongo and save to s3

- `ssh -i "tori-wind-db.pem" ubuntu@ec2-54-82-42-174.compute-1.amazonaws.com`
- `sudo docker run --rm --net=host --env-file=./.env 125853090487.dkr.ecr.us-east-1.amazonaws.com/tori-download-wind-power`
