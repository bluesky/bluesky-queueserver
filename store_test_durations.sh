#!/usr/bin/env bash

USE_IPYKERNEL=true pixi run --environment=py314 pytest -vvv --store-durations
