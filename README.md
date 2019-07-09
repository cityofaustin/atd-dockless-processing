# atd-dockless-processing

This repository contains Python scripts for processing dockless mobility data from [MDS](https://github.com/CityOfLosAngeles/mobility-data-specification)-compliant feeds.

The data processing is addressed in two steps:

- `get_data.py` downloads MDS data and loads it to a [PostgREST](http://postgrest.org) endpoint.

- `pt_in_poly.py` geocodes trip data with council district and census tract attributes

## Deployment Pipeline

We are currently using CircleCI to automatically rebuild the container on every update to this repo, and autonomously deploy that image to Docker-hub. When working in the production or master branches, there are some considerations to keep in mind:

- You will need someone to review your changes for Production or Master branches.
- The production branch is tagged as `latest` in dockerhub, which means it will be the default image and "source of truth".
- Ideally you will want to merge to master, which will cause CircleCI to create a `master` tag in dockerhub, which can be used for additional testing or troubleshooting.
- Any code merged to production, should be production-worthy already. Test thoroughly in master or any other branch before merging in any additional code.

### Development

Feel free to create a new branch, and commit/push as many times as you need. The pipeline will create a new docker image (if it does not exist), or update the existing image in dockerhub. *Your branch name will be used as the identifying tag in docker hub.*

**For example:**

Say you create a branch with the name `123-atd-updatedcode`, as soon as the branch is created CircleCI will begin building the docker image, and tag it as `atddocker/atd-dockless-processing:123-atd-updatedcode` , then it will upload it to Docker hub. If there is already an image with that tag, it will simply update it.
